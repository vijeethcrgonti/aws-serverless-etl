"""
transform_orders.py  —  glue/scripts/
AWS Glue 4.0 PySpark job.
Reads raw orders from S3, applies schema validation, deduplication,
PII masking, and writes Parquet to the processed S3 bucket.
"""

import hashlib
import sys
from datetime import datetime

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "source_bucket",
        "source_key",
        "source_type",
        "output_bucket",
    ],
)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

logger = glueContext.get_logger()

ORDERS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), nullable=False),
        StructField("customer_id", StringType(), nullable=False),
        StructField("store_id", StringType(), nullable=True),
        StructField("product_id", StringType(), nullable=True),
        StructField("quantity", IntegerType(), nullable=True),
        StructField("unit_price", DecimalType(10, 2), nullable=True),
        StructField("order_amount", DecimalType(12, 2), nullable=True),
        StructField("order_date", TimestampType(), nullable=True),
        StructField("status", StringType(), nullable=True),
        StructField("created_at", TimestampType(), nullable=True),
    ]
)


def read_raw(source_bucket: str, source_key: str, source_type: str) -> DataFrame:
    s3_path = f"s3://{source_bucket}/{source_key}"
    ext = source_key.rsplit(".", 1)[-1].lower()

    logger.info(f"Reading {ext} from {s3_path}")

    if ext == "json":
        df = spark.read.schema(ORDERS_SCHEMA).json(s3_path)
    elif ext == "csv":
        df = spark.read.schema(ORDERS_SCHEMA).csv(s3_path, header=True)
    elif ext == "parquet":
        df = spark.read.schema(ORDERS_SCHEMA).parquet(s3_path)
    else:
        raise ValueError(f"Unsupported format: {ext}")

    logger.info(f"Read {df.count()} raw records")
    return df


def validate_schema(df: DataFrame) -> DataFrame:
    before = df.count()
    df = (
        df.filter(F.col("order_id").isNotNull())
        .filter(F.col("customer_id").isNotNull())
        .filter(F.col("order_amount").isNotNull())
        .filter(F.col("order_amount") > 0)
        .filter(F.col("order_date").isNotNull())
    )
    after = df.count()
    logger.info(f"Schema validation dropped {before - after} records")
    return df


def deduplicate(df: DataFrame) -> DataFrame:
    from pyspark.sql.window import Window

    window = Window.partitionBy("order_id").orderBy(F.col("created_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def mask_pii(df: DataFrame) -> DataFrame:
    hash_udf = F.udf(lambda v: hashlib.sha256(v.encode()).hexdigest() if v else None)
    return df.withColumn("customer_id", hash_udf(F.col("customer_id")))


def enrich(df: DataFrame) -> DataFrame:
    return (
        df.withColumn("status", F.upper(F.trim(F.col("status"))))
        .withColumn("order_amount", F.round(F.col("order_amount"), 2))
        .withColumn("order_year", F.year("order_date"))
        .withColumn("order_month", F.month("order_date"))
        .withColumn("order_day", F.dayofmonth("order_date"))
        .withColumn("_processed_at", F.lit(datetime.utcnow().isoformat()))
        .withColumn("_job_run_id", F.lit(args["JOB_NAME"]))
    )


def write_output(df: DataFrame, output_bucket: str, source_type: str):
    output_path = f"s3://{output_bucket}/processed/{source_type}"
    logger.info(f"Writing {df.count()} records to {output_path}")
    (
        df.write.mode("overwrite")
        .partitionBy("order_year", "order_month", "order_day")
        .option("compression", "snappy")
        .parquet(output_path)
    )
    logger.info("Write complete")


# ── Main ───────────────────────────────────────────────────────────────────────

df = read_raw(args["source_bucket"], args["source_key"], args["source_type"])
df = validate_schema(df)
df = deduplicate(df)
df = mask_pii(df)
df = enrich(df)
write_output(df, args["output_bucket"], args["source_type"])

job.commit()
