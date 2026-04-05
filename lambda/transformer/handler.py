"""
handler.py  —  lambda/transformer/
Triggered after Glue job completes (via EventBridge rule on Glue job state SUCCEEDED).
Executes COPY + MERGE into Redshift using a staging table pattern.
Runs post-load row count validation before marking job complete.
"""

import json
import logging
import os

import boto3
import redshift_connector

logger = logging.getLogger()
logger.setLevel(logging.INFO)

secretsmanager = boto3.client("secretsmanager")
sns = boto3.client("sns")

SNS_TOPIC_ARN = os.environ["SNS_TOPIC_ARN"]
REDSHIFT_SECRET_ARN = os.environ["REDSHIFT_SECRET_ARN"]
REDSHIFT_DATABASE = os.environ["REDSHIFT_DATABASE"]
REDSHIFT_IAM_ROLE = os.environ["REDSHIFT_IAM_ROLE"]

SOURCE_TABLE_MAP = {
    "orders": "public.orders",
    "products": "public.products",
    "customers": "public.customers",
    "inventory": "public.inventory",
}

STAGING_TABLE_MAP = {
    "orders": "staging.orders",
    "products": "staging.products",
    "customers": "staging.customers",
    "inventory": "staging.inventory",
}

UPSERT_KEYS = {
    "orders": "order_id",
    "products": "product_id",
    "customers": "customer_id",
    "inventory": "inventory_id",
}


def get_redshift_creds() -> dict:
    secret = secretsmanager.get_secret_value(SecretId=REDSHIFT_SECRET_ARN)
    return json.loads(secret["SecretString"])


def get_connection(creds: dict):
    return redshift_connector.connect(
        host=creds["host"],
        database=REDSHIFT_DATABASE,
        user=creds["username"],
        password=creds["password"],
        port=int(creds.get("port", 5439)),
    )


def truncate_staging(cursor, staging_table: str):
    cursor.execute(f"TRUNCATE TABLE {staging_table};")
    logger.info(f"Truncated {staging_table}")


def copy_to_staging(cursor, s3_path: str, staging_table: str):
    sql = f"""
        COPY {staging_table}
        FROM '{s3_path}'
        IAM_ROLE '{REDSHIFT_IAM_ROLE}'
        FORMAT AS PARQUET;
    """
    logger.info(f"COPY: {s3_path} -> {staging_table}")
    cursor.execute(sql)


def merge_into_target(cursor, source_type: str):
    staging = STAGING_TABLE_MAP[source_type]
    target = SOURCE_TABLE_MAP[source_type]
    key = UPSERT_KEYS[source_type]

    sql = f"""
        MERGE INTO {target}
        USING {staging} AS src
        ON {target}.{key} = src.{key}
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *;
    """
    cursor.execute(sql)
    logger.info(f"MERGE complete: {staging} -> {target}")


def validate_load(cursor, staging_table: str, target_table: str) -> bool:
    cursor.execute(f"SELECT COUNT(*) FROM {staging_table};")
    staging_count = cursor.fetchone()[0]

    cursor.execute(f"SELECT COUNT(*) FROM {target_table} WHERE _loaded_at >= GETDATE() - INTERVAL '1 hour';")
    target_count = cursor.fetchone()[0]

    logger.info(f"Staging rows: {staging_count} | Recent target rows: {target_count}")

    if staging_count == 0:
        logger.warning("Staging table is empty after COPY — possible load failure")
        return False
    return True


def lambda_handler(event, context):
    logger.info(f"Event: {json.dumps(event)}")

    detail = event.get("detail", {})
    job_name = detail.get("jobName", "")
    job_run_id = detail.get("jobRunId", "")
    state = detail.get("state", "")
    source_type = detail.get("arguments", {}).get("--source_type", "")
    output_bucket = detail.get("arguments", {}).get("--output_bucket", "")
    source_key = detail.get("arguments", {}).get("--source_key", "")

    if state != "SUCCEEDED":
        logger.warning(f"Glue job {job_run_id} ended with state {state} — skipping load")
        return {"statusCode": 200, "body": "Skipped — job did not succeed"}

    if source_type not in SOURCE_TABLE_MAP:
        logger.error(f"Unknown source_type: {source_type}")
        return {"statusCode": 400, "body": f"Unknown source: {source_type}"}

    processed_prefix = source_key.rsplit("/", 1)[0]
    s3_path = f"s3://{output_bucket}/processed/{processed_prefix}/"

    creds = get_redshift_creds()
    conn = get_connection(creds)
    conn.autocommit = False
    cursor = conn.cursor()

    try:
        truncate_staging(cursor, STAGING_TABLE_MAP[source_type])
        copy_to_staging(cursor, s3_path, STAGING_TABLE_MAP[source_type])
        valid = validate_load(cursor, STAGING_TABLE_MAP[source_type], SOURCE_TABLE_MAP[source_type])

        if not valid:
            conn.rollback()
            raise ValueError("Post-COPY validation failed — rolling back")

        merge_into_target(cursor, source_type)
        conn.commit()
        logger.info("Load complete and committed")

        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"ETL Load Success: {source_type}",
            Message=f"Glue run {job_run_id} loaded successfully into {SOURCE_TABLE_MAP[source_type]}",
        )
        return {"statusCode": 200, "body": "Load complete"}

    except Exception as e:
        conn.rollback()
        logger.error(f"Load failed: {e}")
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"ETL Load FAILED: {source_type}",
            Message=str(e),
        )
        raise
    finally:
        cursor.close()
        conn.close()
