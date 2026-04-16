"""
handler.py  —  lambda/trigger/
Fired by S3 PutObject events on the raw bucket.
Validates the incoming file, classifies the source type,
and kicks off the appropriate AWS Glue job.
"""

import json
import logging
import os
import urllib.parse

import boto3
import botocore

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client("glue")
s3 = boto3.client("s3")
sns = boto3.client("sns")

GLUE_JOB_NAME = os.environ["GLUE_JOB_NAME"]
SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
PROCESSED_BUCKET = os.environ["PROCESSED_BUCKET"]

SUPPORTED_SOURCES = {"orders", "products", "customers", "inventory"}
SUPPORTED_FORMATS = {".json", ".csv", ".parquet"}


def parse_s3_event(event: dict) -> list[dict]:
    records = []
    for rec in event.get("Records", []):
        bucket = rec["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
        size = rec["s3"]["object"].get("size", 0)
        records.append({"bucket": bucket, "key": key, "size": size})
    return records


def classify_source(key: str) -> str | None:
    """Extract source type from S3 key prefix. e.g. orders/2024/01/15/file.json -> orders"""
    parts = key.split("/")
    if parts:
        source = parts[0].lower()
        return source if source in SUPPORTED_SOURCES else None
    return None


def validate_file(bucket: str, key: str, size: int) -> tuple[bool, str]:
    if size == 0:
        return False, "File is empty"

    ext = "." + key.rsplit(".", 1)[-1].lower() if "." in key else ""
    if ext not in SUPPORTED_FORMATS:
        return False, f"Unsupported file format: {ext}"

    try:
        s3.head_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError as e:
        return False, f"S3 head_object failed: {e}"

    return True, "OK"


def trigger_glue_job(bucket: str, key: str, source: str) -> str:
    response = glue.start_job_run(
        JobName=GLUE_JOB_NAME,
        Arguments={
            "--source_bucket": bucket,
            "--source_key": key,
            "--source_type": source,
            "--output_bucket": PROCESSED_BUCKET,
            "--enable-metrics": "true",
            "--enable-continuous-cloudwatch-log": "true",
        },
    )
    run_id = response["JobRunId"]
    logger.info(
        f"Started Glue job {GLUE_JOB_NAME} run {run_id} for s3://{bucket}/{key}"
    )
    return run_id


def send_alert(subject: str, message: str):
    sns.publish(
        TopicArn=SNS_TOPIC_ARN,
        Subject=subject,
        Message=message,
    )


def lambda_handler(event, context):
    logger.info(f"Event received: {json.dumps(event)}")
    records = parse_s3_event(event)
    results = []

    for record in records:
        bucket, key, size = record["bucket"], record["key"], record["size"]

        valid, reason = validate_file(bucket, key, size)
        if not valid:
            msg = f"Validation failed for s3://{bucket}/{key}: {reason}"
            logger.warning(msg)
            send_alert("ETL Validation Failure", msg)
            results.append(
                {"key": key, "status": "validation_failed", "reason": reason}
            )
            continue

        source = classify_source(key)
        if not source:
            msg = f"Unknown source for key: {key}"
            logger.warning(msg)
            results.append({"key": key, "status": "unknown_source"})
            continue

        try:
            run_id = trigger_glue_job(bucket, key, source)
            results.append({"key": key, "status": "glue_triggered", "run_id": run_id})
        except Exception as e:
            logger.error(f"Failed to trigger Glue for {key}: {e}")
            send_alert("ETL Glue Trigger Failure", str(e))
            results.append({"key": key, "status": "error", "error": str(e)})

    return {"statusCode": 200, "body": json.dumps(results)}
