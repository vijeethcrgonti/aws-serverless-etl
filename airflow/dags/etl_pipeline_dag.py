"""
etl_pipeline_dag.py  —  airflow/dags/
MWAA Airflow DAG that orchestrates the full serverless ETL pipeline.
Supports manual backfill via run_date param. Runs daily at 03:00 UTC.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator

import boto3

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "email_on_failure": True,
    "email": ["de-alerts@example.com"],
}

RAW_BUCKET = "{{ var.value.ETL_RAW_BUCKET }}"
PROCESSED_BUCKET = "{{ var.value.ETL_PROCESSED_BUCKET }}"
GLUE_JOB_NAME = "{{ var.value.ETL_GLUE_JOB_NAME }}"
AWS_CONN_ID = "aws_default"


def check_raw_file_exists(**context) -> bool:
    """Short-circuit if no raw file landed for this date."""
    _date = context["params"].get("run_date") or context["ds"]
    s3 = boto3.client("s3")
    bucket = context["var"]["value"]["ETL_RAW_BUCKET"]
    prefix = f"orders/{date.replace('-', '/')}/"
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
    found = resp.get("KeyCount", 0) > 0
    if not found:
        context["task_instance"].log.warning(
            f"No files found at s3://{bucket}/{prefix} — skipping run"
        )
    return found


def validate_processed_output(**context):
    _date = context["params"].get("run_date") or context["ds"]
    s3 = boto3.client("s3")
    bucket = context["var"]["value"]["ETL_PROCESSED_BUCKET"]
    prefix = "processed/orders/"
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
    count = resp.get("KeyCount", 0)
    context["task_instance"].log.info(f"Processed output files found: {count}")
    if count == 0:
        raise ValueError(
            "No processed files found after Glue job — pipeline may have failed silently"
        )


with DAG(
    dag_id="aws_serverless_etl",
    default_args=DEFAULT_ARGS,
    description="Lambda + Glue + Redshift serverless ETL pipeline",
    schedule_interval="0 3 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["aws", "glue", "lambda", "redshift"],
    params={"run_date": None},
) as dag:

    check_raw = ShortCircuitOperator(
        task_id="check_raw_file_exists",
        python_callable=check_raw_file_exists,
        provide_context=True,
    )

    run_glue = GlueJobOperator(
        task_id="run_glue_transform",
        job_name=GLUE_JOB_NAME,
        script_args={
            "--source_type": "orders",
            "--source_bucket": RAW_BUCKET,
            "--output_bucket": PROCESSED_BUCKET,
        },
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
        verbose=True,
    )

    validate_output = PythonOperator(
        task_id="validate_processed_output",
        python_callable=validate_processed_output,
        provide_context=True,
    )

    run_crawler = GlueCrawlerOperator(
        task_id="run_glue_crawler",
        config={"Name": "etl-processed-crawler-dev"},
        aws_conn_id=AWS_CONN_ID,
        wait_for_completion=True,
    )

    check_raw >> run_glue >> validate_output >> run_crawler
