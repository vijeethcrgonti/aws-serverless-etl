"""
monitoring_stack.py  —  cdk/lib/
CloudWatch alarms, dashboard, and SNS alerts for the ETL pipeline.
"""

import aws_cdk as cdk
from aws_cdk import (
    aws_cloudwatch as cw,
    aws_cloudwatch_actions as cw_actions,
    aws_lambda as lambda_,
    aws_sns as sns,
)
from constructs import Construct


class MonitoringStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage: str,
        lambda_trigger_fn: lambda_.Function,
        lambda_transformer_fn: lambda_.Function,
        glue_job_name: str,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        alert_topic = sns.Topic(
            self, "MonitoringTopic",
            topic_name=f"etl-monitoring-{stage}",
        )

        # ── Lambda Error Alarms ────────────────────────────────────────────────

        for fn, name in [(lambda_trigger_fn, "Trigger"), (lambda_transformer_fn, "Transformer")]:
            alarm = cw.Alarm(
                self, f"{name}ErrorAlarm",
                alarm_name=f"etl-{name.lower()}-errors-{stage}",
                metric=fn.metric_errors(period=cdk.Duration.minutes(5)),
                threshold=1,
                evaluation_periods=1,
                comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
                treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
            )
            alarm.add_alarm_action(cw_actions.SnsAction(alert_topic))

        # ── Lambda Duration Alarms ─────────────────────────────────────────────

        cw.Alarm(
            self, "TransformerDurationAlarm",
            alarm_name=f"etl-transformer-duration-{stage}",
            metric=lambda_transformer_fn.metric_duration(period=cdk.Duration.minutes(5)),
            threshold=4 * 60 * 1000,  # 4 minutes in ms
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_THRESHOLD,
        ).add_alarm_action(cw_actions.SnsAction(alert_topic))

        # ── Glue Job Failure Alarm ─────────────────────────────────────────────

        cw.Alarm(
            self, "GlueFailureAlarm",
            alarm_name=f"etl-glue-failures-{stage}",
            metric=cw.Metric(
                namespace="Glue",
                metric_name="glue.driver.aggregate.numFailedTask",
                dimensions_map={"JobName": glue_job_name},
                period=cdk.Duration.minutes(15),
                statistic="Sum",
            ),
            threshold=1,
            evaluation_periods=1,
            comparison_operator=cw.ComparisonOperator.GREATER_THAN_OR_EQUAL_TO_THRESHOLD,
            treat_missing_data=cw.TreatMissingData.NOT_BREACHING,
        ).add_alarm_action(cw_actions.SnsAction(alert_topic))

        # ── Dashboard ──────────────────────────────────────────────────────────

        dashboard = cw.Dashboard(
            self, "ETLDashboard",
            dashboard_name=f"ETL-Pipeline-{stage}",
        )

        dashboard.add_widgets(
            cw.GraphWidget(
                title="Lambda Invocations",
                left=[
                    lambda_trigger_fn.metric_invocations(),
                    lambda_transformer_fn.metric_invocations(),
                ],
            ),
            cw.GraphWidget(
                title="Lambda Errors",
                left=[
                    lambda_trigger_fn.metric_errors(),
                    lambda_transformer_fn.metric_errors(),
                ],
            ),
            cw.GraphWidget(
                title="Lambda Duration (ms)",
                left=[
                    lambda_trigger_fn.metric_duration(),
                    lambda_transformer_fn.metric_duration(),
                ],
            ),
        )

        cdk.CfnOutput(self, "DashboardName", value=f"ETL-Pipeline-{stage}")
        cdk.CfnOutput(self, "AlertTopicArn", value=alert_topic.topic_arn)
