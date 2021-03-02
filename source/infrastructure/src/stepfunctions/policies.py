# #####################################################################################################################
#  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                            #
#                                                                                                                     #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. A copy of the License is located at                                                              #
#                                                                                                                     #
#  http://www.apache.org/licenses/LICENSE-2.0                                                                         #
#                                                                                                                     #
#  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES  #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions     #
#  and limitations under the License.                                                                                 #
# #####################################################################################################################

from dataclasses import field, dataclass
from typing import Dict

import aws_cdk.aws_iam as iam
from aws_cdk.aws_s3 import IBucket
from aws_cdk.core import CfnResource, Aws, CfnCondition

from policies import Policies
from solutions.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression


@dataclass
class SfnPolicies(Policies):
    _forecast_roles: Dict[str, iam.Role] = field(
        init=False, repr=False, default_factory=dict
    )
    forecast_kms_read_policy: iam.Policy = field(init=False, repr=False, default=None)
    forecast_kms_write_policy: iam.Policy = field(init=False, repr=False, default=None)

    def forecast_s3_read_role(self, name: str, data_bucket_name_resource: CfnResource):
        return self._forecast_s3_role(
            name, data_bucket_name_resource, read=True, write=False
        )

    def forecast_s3_read_write_role(
        self, name: str, data_bucket_name_resource: CfnResource
    ):
        return self._forecast_s3_role(
            name, data_bucket_name_resource, read=True, write=True
        )

    def create_forecast_kms_write_policy(
        self, forecast_kms: CfnCondition, forecast_kms_key_arn: str
    ):
        # KMS requirements from S3: "Protecting Data Using Server-Side Encryption with CMKs[...]"
        # Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html
        if self.forecast_kms_write_policy:
            return self.forecast_kms_write_policy
        self.forecast_kms_write_policy = iam.Policy(
            self.owner,
            "ForecastKmsWriteAccess",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "kms:Encrypt",
                        "kms:ReEncrypt",
                        "kms:Decrypt",
                        "kms:CreateGrant",
                        "kms:RevokeGrant",
                        "kms:RetireGrant",
                        "kms:ListGrants",
                        "kms:GenerateDataKey",
                        "kms:DescribeKey",
                    ],
                    resources=[forecast_kms_key_arn],
                )
            ],
        )
        self.forecast_kms_write_policy.node.default_child.cfn_options.condition = (
            forecast_kms
        )
        return self.forecast_kms_write_policy

    def create_forecast_kms_read_policy(
        self, forecast_kms: CfnCondition, forecast_kms_key_arn: str
    ):
        # KMS requirements from S3: "Protecting Data Using Server-Side Encryption with CMKs[...]"
        # Source: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html
        if self.forecast_kms_read_policy:
            return self.forecast_kms_read_policy
        self.forecast_kms_read_policy = iam.Policy(
            self.owner,
            "ForecastKmsReadAccess",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "kms:Decrypt",
                        "kms:DescribeKey",
                        "kms:GenerateDataKey",
                    ],
                    resources=[forecast_kms_key_arn],
                )
            ],
        )
        self.forecast_kms_read_policy.node.default_child.cfn_options.condition = (
            forecast_kms
        )
        return self.forecast_kms_read_policy

    def _forecast_s3_role(
        self, name: str, data_bucket_name_resource: CfnResource, read=False, write=False
    ):
        """
        Create a read or read/write access role for Amazon Forecast. NOTE: Amazon Forecast does not currently support
        AWS VPCs and VPC Endpoints for S3 access at the time of publication
        https://docs.aws.amazon.com/forecast/latest/dg/aws-forecast-iam-roles.html for required permissions
        :param name: The name of the role
        :param data_bucket_name_resource: the data bucket naming custom resource
        :return: the created (or existing) named IAM Role
        """
        actions = []
        mode = ""
        if read:
            actions.extend(
                [
                    "s3:Get*",
                    "s3:List*",
                ]
            )
            mode += "Read"
        if write:
            actions.extend(
                [
                    "s3:PutObject",
                ]
            )
            mode += "Write"
        if not read and not write:
            raise ValueError(
                "forecast s3 role must have read, write, or both set to True"
            )

        role_id = f"{name}ForecastS3{mode}Role"
        try:
            return self._forecast_roles[role_id]
        except KeyError:
            forecast_role = iam.Role(
                self.owner,
                role_id,
                assumed_by=iam.ServicePrincipal("forecast.amazonaws.com"),
                inline_policies={
                    role_id: iam.PolicyDocument(
                        statements=[
                            iam.PolicyStatement(
                                effect=iam.Effect.ALLOW,
                                actions=actions,
                                resources=self.data_bucket_resources(
                                    data_bucket_name_resource
                                ),
                            )
                        ]
                    )
                },
            )
            # associate KMS policies for read and write as required
            if read:
                self.forecast_kms_read_policy.attach_to_role(forecast_role)
            if write:
                self.forecast_kms_write_policy.attach_to_role(forecast_role)

        self._forecast_roles[role_id] = forecast_role
        return self._forecast_roles[role_id]

    def data_bucket_resources(self, data_bucket_name_resource: CfnResource):
        data_bucket_name = data_bucket_name_resource.get_att("Name").to_string()
        return [
            f"arn:{Aws.PARTITION}:s3:::{data_bucket_name}/*",
            f"arn:{Aws.PARTITION}:s3:::{data_bucket_name}",
        ]

    def s3_bucket_read_policy(self, name: str, data_bucket_name_resource: CfnResource):
        return iam.Policy(
            self.owner,
            f"{name}S3BucketAccess",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:ListObjects",
                    ],
                    resources=self.data_bucket_resources(data_bucket_name_resource),
                )
            ],
        )

    def forecast_read_write_policy(self, name):
        policy = iam.Policy(
            self.owner,
            f"{name}ForecastAccess",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "forecast:Describe*",
                        "forecast:List*",
                        "forecast:Create*",
                        "forecast:Update*",
                        "forecast:TagResource",
                    ],
                    resources=["*"],
                )
            ],
        )
        add_cfn_nag_suppressions(
            policy.node.default_child,
            [
                CfnNagSuppression(
                    "W12",
                    "Require access to all resources; Not all Amazon Forecast resources support resource based policy",
                )
            ],
        )
        return policy

    def athena_access(self, workgroup_name):
        return iam.Policy(
            self.owner,
            "AthenaAccess",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "athena:StartQueryExecution",
                        "athena:GetQueryExecution",
                        "athena:GetQueryResults",
                    ],
                    resources=[
                        f"arn:{Aws.PARTITION}:athena:{Aws.REGION}:{Aws.ACCOUNT_ID}:workgroup/{workgroup_name}"
                    ],
                )
            ],
        )

    def glue_access(
        self,
        catalog: CfnResource,
        athena_bucket: IBucket,
        data_bucket_name_resource: CfnResource,
    ):
        return iam.Policy(
            self.owner,
            "GlueAccess",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "glue:GetDatabase",
                        "glue:GetTable",
                        "glue:GetPartitions",
                        "glue:DeleteTable",  # required to delete temporary tables
                        "glue:CreateTable",
                        "glue:BatchCreatePartition",
                    ],
                    resources=[
                        f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog",
                        f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:{catalog.ref}",
                        f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{catalog.ref}/*",
                        f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{catalog.ref}",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:ListObjects",
                        "s3:PutObject",
                        "s3:CreateMultipartUpload",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload",
                    ],
                    resources=[
                        athena_bucket.arn_for_objects("*"),
                        athena_bucket.bucket_arn,
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:ListObjects",
                        "s3:PutObject",
                        "s3:CreateMultipartUpload",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload",
                    ],
                    resources=self.data_bucket_resources(data_bucket_name_resource),
                ),
            ],
        )

    def quicksight_access(self):
        policy = iam.Policy(
            self.owner,
            "QuickSightAccess",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "quicksight:CreateAnalysis",
                        "quicksight:CreateDataSet",
                        "quicksight:CreateDataSource",
                        "quicksight:UpdateDataSource",
                        "quicksight:UpdateDataSourcePermissions",
                        "quicksight:Describe*",
                        "quicksight:Get*",
                        "quicksight:List*",
                        "quicksight:PassDataSet",
                        "quicksight:PassDataSource",
                        "quicksight:RestoreAnalysis",
                        "quicksight:SearchAnalyses",
                    ],
                    resources=["*"],
                )
            ],
        )
        add_cfn_nag_suppressions(
            policy.node.default_child,
            [
                CfnNagSuppression(
                    "W12",
                    "Require access to all resources; Not all Amazon Forecast resources support resource based policy",
                )
            ],
        )

        return policy
