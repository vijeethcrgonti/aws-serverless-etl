"""
glue_stack.py  —  cdk/lib/
Provisions Glue job, IAM execution role, Data Catalog database, and crawler.
"""

import aws_cdk as cdk
from aws_cdk import (
    aws_glue as glue,
    aws_iam as iam,
    aws_s3 as s3,
)
from constructs import Construct


class GlueStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage: str,
        raw_bucket: s3.Bucket,
        processed_bucket: s3.Bucket,
        scripts_bucket: s3.Bucket,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # ── IAM Role ───────────────────────────────────────────────────────────

        glue_role = iam.Role(
            self, "GlueRole",
            role_name=f"GlueETLRole-{stage}",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
            ],
        )
        raw_bucket.grant_read(glue_role)
        processed_bucket.grant_read_write(glue_role)
        scripts_bucket.grant_read(glue_role)

        # ── Data Catalog ───────────────────────────────────────────────────────

        database = glue.CfnDatabase(
            self, "GlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name=f"etl_catalog_{stage}",
                description="Glue Data Catalog for serverless ETL pipeline",
            ),
        )

        # ── Glue Job ───────────────────────────────────────────────────────────

        self.job_name = f"etl-transform-orders-{stage}"

        glue.CfnJob(
            self, "TransformOrdersJob",
            name=self.job_name,
            role=glue_role.role_arn,
            command=glue.CfnJob.JobCommandProperty(
                name="glueetl",
                python_version="3",
                script_location=f"s3://{scripts_bucket.bucket_name}/glue-scripts/transform_orders.py",
            ),
            glue_version="4.0",
            number_of_workers=5,
            worker_type="G.1X",
            max_retries=1,
            timeout=60,
            default_arguments={
                "--job-language": "python",
                "--job-bookmark-option": "job-bookmark-enable",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--enable-spark-ui": "true",
                "--spark-event-logs-path": f"s3://{processed_bucket.bucket_name}/spark-logs/",
                "--TempDir": f"s3://{processed_bucket.bucket_name}/glue-temp/",
            },
        )

        # ── Crawler ────────────────────────────────────────────────────────────

        glue.CfnCrawler(
            self, "ProcessedCrawler",
            name=f"etl-processed-crawler-{stage}",
            role=glue_role.role_arn,
            database_name=f"etl_catalog_{stage}",
            targets=glue.CfnCrawler.TargetsProperty(
                s3_targets=[
                    glue.CfnCrawler.S3TargetProperty(
                        path=f"s3://{processed_bucket.bucket_name}/processed/",
                    )
                ]
            ),
            schedule=glue.CfnCrawler.ScheduleProperty(
                schedule_expression="cron(0 3 * * ? *)"
            ),
            configuration='{"Version":1.0,"CrawlerOutput":{"Partitions":{"AddOrUpdateBehavior":"InheritFromTable"}}}',
        )

        cdk.CfnOutput(self, "GlueJobName", value=self.job_name)
        cdk.CfnOutput(self, "GlueDatabaseName", value=f"etl_catalog_{stage}")
