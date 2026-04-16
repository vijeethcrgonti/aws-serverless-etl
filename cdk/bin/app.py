#!/usr/bin/env python3
"""
app.py  —  cdk/bin/
CDK application entry point. Deploys all stacks for the AWS Serverless ETL pipeline.
"""

import aws_cdk as cdk

from lib.network_stack import NetworkStack
from lib.storage_stack import StorageStack
from lib.glue_stack import GlueStack
from lib.lambda_stack import LambdaStack
from lib.redshift_stack import RedshiftStack
from lib.monitoring_stack import MonitoringStack

app = cdk.App()

env = cdk.Environment(
    account=app.node.try_get_context("account"),
    region=app.node.try_get_context("region") or "us-east-1",
)

stage = app.node.try_get_context("stage") or "dev"

network = NetworkStack(app, f"NetworkStack-{stage}", env=env, stage=stage)

storage = StorageStack(app, f"StorageStack-{stage}", env=env, stage=stage)

glue = GlueStack(
    app,
    f"GlueStack-{stage}",
    env=env,
    stage=stage,
    raw_bucket=storage.raw_bucket,
    processed_bucket=storage.processed_bucket,
    scripts_bucket=storage.scripts_bucket,
)

redshift = RedshiftStack(
    app,
    f"RedshiftStack-{stage}",
    env=env,
    stage=stage,
    vpc=network.vpc,
    processed_bucket=storage.processed_bucket,
)

lambda_stack = LambdaStack(
    app,
    f"LambdaStack-{stage}",
    env=env,
    stage=stage,
    raw_bucket=storage.raw_bucket,
    processed_bucket=storage.processed_bucket,
    glue_job_name=glue.job_name,
    redshift_secret=redshift.secret,
)

monitoring = MonitoringStack(
    app,
    f"MonitoringStack-{stage}",
    env=env,
    stage=stage,
    lambda_trigger_fn=lambda_stack.trigger_fn,
    lambda_transformer_fn=lambda_stack.transformer_fn,
    glue_job_name=glue.job_name,
)

app.synth()
