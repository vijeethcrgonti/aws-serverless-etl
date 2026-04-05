"""
lambda_stack.py  —  cdk/lib/
Provisions Lambda functions for trigger and transformer,
IAM roles, S3 event notifications, and EventBridge rules.
"""

import aws_cdk as cdk
from aws_cdk import (
    aws_events as events,
    aws_events_targets as targets,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_events,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
    aws_sns as sns,
)
from constructs import Construct


class LambdaStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage: str,
        raw_bucket: s3.Bucket,
        processed_bucket: s3.Bucket,
        glue_job_name: str,
        redshift_secret: secretsmanager.Secret,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        self.alert_topic = sns.Topic(
            self, "AlertTopic",
            topic_name=f"etl-alerts-{stage}",
            display_name="ETL Pipeline Alerts",
        )

        # ── Shared Lambda Layer ────────────────────────────────────────────────

        shared_layer = lambda_.LayerVersion(
            self, "SharedLayer",
            code=lambda_.Code.from_asset("../lambda/layer"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description="Shared utilities: boto3, redshift_connector",
        )

        # ── Trigger Lambda ─────────────────────────────────────────────────────

        trigger_role = iam.Role(
            self, "TriggerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
            ],
        )
        raw_bucket.grant_read(trigger_role)
        trigger_role.add_to_policy(iam.PolicyStatement(
            actions=["glue:StartJobRun"],
            resources=[f"arn:aws:glue:{self.region}:{self.account}:job/{glue_job_name}"],
        ))
        trigger_role.add_to_policy(iam.PolicyStatement(
            actions=["sns:Publish"],
            resources=[self.alert_topic.topic_arn],
        ))

        self.trigger_fn = lambda_.Function(
            self, "TriggerFunction",
            function_name=f"etl-trigger-{stage}",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("../lambda/trigger"),
            layers=[shared_layer],
            role=trigger_role,
            timeout=cdk.Duration.seconds(60),
            memory_size=256,
            environment={
                "GLUE_JOB_NAME": glue_job_name,
                "SNS_TOPIC_ARN": self.alert_topic.topic_arn,
                "PROCESSED_BUCKET": processed_bucket.bucket_name,
            },
            tracing=lambda_.Tracing.ACTIVE,
        )

        # S3 trigger: fire on any PutObject to raw bucket
        self.trigger_fn.add_event_source(
            lambda_events.S3EventSource(
                raw_bucket,
                events=[s3.EventType.OBJECT_CREATED],
            )
        )

        # ── Transformer Lambda ─────────────────────────────────────────────────

        transformer_role = iam.Role(
            self, "TransformerRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole"),
            ],
        )
        processed_bucket.grant_read(transformer_role)
        redshift_secret.grant_read(transformer_role)
        transformer_role.add_to_policy(iam.PolicyStatement(
            actions=["sns:Publish"],
            resources=[self.alert_topic.topic_arn],
        ))

        self.transformer_fn = lambda_.Function(
            self, "TransformerFunction",
            function_name=f"etl-transformer-{stage}",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset("../lambda/transformer"),
            layers=[shared_layer],
            role=transformer_role,
            timeout=cdk.Duration.minutes(5),
            memory_size=512,
            environment={
                "SNS_TOPIC_ARN": self.alert_topic.topic_arn,
                "REDSHIFT_SECRET_ARN": redshift_secret.secret_arn,
                "REDSHIFT_DATABASE": "etl_db",
                "REDSHIFT_IAM_ROLE": f"arn:aws:iam::{self.account}:role/RedshiftS3Role-{stage}",
            },
            tracing=lambda_.Tracing.ACTIVE,
        )

        # EventBridge: fire transformer when Glue job SUCCEEDS
        glue_success_rule = events.Rule(
            self, "GlueSuccessRule",
            event_pattern=events.EventPattern(
                source=["aws.glue"],
                detail_type=["Glue Job State Change"],
                detail={"jobName": [glue_job_name], "state": ["SUCCEEDED"]},
            ),
        )
        glue_success_rule.add_target(targets.LambdaFunction(self.transformer_fn))

        cdk.CfnOutput(self, "TriggerFunctionArn", value=self.trigger_fn.function_arn)
        cdk.CfnOutput(self, "TransformerFunctionArn", value=self.transformer_fn.function_arn)
        cdk.CfnOutput(self, "AlertTopicArn", value=self.alert_topic.topic_arn)
