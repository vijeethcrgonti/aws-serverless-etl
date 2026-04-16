"""
storage_stack.py  —  cdk/lib/
Provisions S3 buckets: raw, processed, archive, and glue-scripts.
Enforces encryption, versioning, and lifecycle policies.
"""

import aws_cdk as cdk
from aws_cdk import aws_s3 as s3
from constructs import Construct


class StorageStack(cdk.Stack):
    def __init__(self, scope: Construct, construct_id: str, stage: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        self.raw_bucket = s3.Bucket(
            self,
            "RawBucket",
            bucket_name=f"etl-raw-{stage}-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="ArchiveRawAfter30Days",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.INTELLIGENT_TIERING,
                            transition_after=cdk.Duration.days(30),
                        )
                    ],
                )
            ],
            event_bridge_enabled=True,
        )

        self.processed_bucket = s3.Bucket(
            self,
            "ProcessedBucket",
            bucket_name=f"etl-processed-{stage}-{self.account}",
            versioned=True,
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.RETAIN,
        )

        self.scripts_bucket = s3.Bucket(
            self,
            "ScriptsBucket",
            bucket_name=f"etl-glue-scripts-{stage}-{self.account}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.DESTROY,
        )

        self.archive_bucket = s3.Bucket(
            self,
            "ArchiveBucket",
            bucket_name=f"etl-archive-{stage}-{self.account}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            removal_policy=cdk.RemovalPolicy.RETAIN,
            lifecycle_rules=[
                s3.LifecycleRule(
                    id="GlacierAfter90Days",
                    transitions=[
                        s3.Transition(
                            storage_class=s3.StorageClass.GLACIER,
                            transition_after=cdk.Duration.days(90),
                        )
                    ],
                )
            ],
        )

        # Outputs
        cdk.CfnOutput(self, "RawBucketName", value=self.raw_bucket.bucket_name)
        cdk.CfnOutput(
            self, "ProcessedBucketName", value=self.processed_bucket.bucket_name
        )
        cdk.CfnOutput(self, "ScriptsBucketName", value=self.scripts_bucket.bucket_name)
