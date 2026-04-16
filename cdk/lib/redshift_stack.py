"""
redshift_stack.py  —  cdk/lib/
Provisions Redshift Serverless namespace, workgroup, and secrets.
"""

import aws_cdk as cdk
from aws_cdk import (
    aws_ec2 as ec2,
    aws_iam as iam,
    aws_redshiftserverless as redshift,
    aws_s3 as s3,
    aws_secretsmanager as secretsmanager,
)
from constructs import Construct


class RedshiftStack(cdk.Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        stage: str,
        vpc: ec2.Vpc,
        processed_bucket: s3.Bucket,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        # ── Redshift IAM Role (for COPY from S3) ───────────────────────────────

        redshift_role = iam.Role(
            self,
            "RedshiftS3Role",
            role_name=f"RedshiftS3Role-{stage}",
            assumed_by=iam.ServicePrincipal("redshift.amazonaws.com"),
        )
        processed_bucket.grant_read(redshift_role)

        # ── Credentials Secret ─────────────────────────────────────────────────

        self.secret = secretsmanager.Secret(
            self,
            "RedshiftSecret",
            secret_name=f"etl/redshift/{stage}",
            generate_secret_string=secretsmanager.SecretStringGenerator(
                secret_string_template='{"username": "etl_admin"}',
                generate_string_key="password",
                exclude_punctuation=True,
                password_length=32,
            ),
        )

        # ── Security Group ─────────────────────────────────────────────────────

        sg = ec2.SecurityGroup(
            self,
            "RedshiftSG",
            vpc=vpc,
            description="Redshift Serverless security group",
            allow_all_outbound=True,
        )
        sg.add_ingress_rule(
            peer=ec2.Peer.ipv4(vpc.vpc_cidr_block),
            connection=ec2.Port.tcp(5439),
            description="Allow Redshift from within VPC",
        )

        # ── Namespace ──────────────────────────────────────────────────────────

        namespace = redshift.CfnNamespace(
            self,
            "Namespace",
            namespace_name=f"etl-namespace-{stage}",
            db_name="etl_db",
            admin_username="etl_admin",
            admin_user_password=self.secret.secret_value_from_json(
                "password"
            ).unsafe_unwrap(),
            iam_roles=[redshift_role.role_arn],
        )

        # ── Workgroup ──────────────────────────────────────────────────────────

        workgroup = redshift.CfnWorkgroup(
            self,
            "Workgroup",
            workgroup_name=f"etl-workgroup-{stage}",
            namespace_name=namespace.namespace_name,
            base_capacity=32,
            publicly_accessible=False,
            subnet_ids=vpc.select_subnets(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS
            ).subnet_ids,
            security_group_ids=[sg.security_group_id],
        )
        workgroup.add_dependency(namespace)

        cdk.CfnOutput(self, "RedshiftSecretArn", value=self.secret.secret_arn)
        cdk.CfnOutput(self, "WorkgroupName", value=f"etl-workgroup-{stage}")
