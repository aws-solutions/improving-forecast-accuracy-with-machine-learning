# #####################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                 #
#                                                                                                                     #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. You may obtain a copy of the License at                                                          #
#                                                                                                                     #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                        #
#                                                                                                                     #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed   #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for  #
#  the specific language governing permissions and limitations under the License.                                     #
# #####################################################################################################################

import jsii
from aws_cdk.aws_iam import (
    IGrantable,
    Role,
    PolicyStatement,
    PolicyDocument,
    ServicePrincipal,
    Policy,
    Effect,
)
from aws_cdk.aws_lambda import Function
from aws_cdk.aws_s3 import Bucket
from aws_cdk.core import (
    Construct,
    CfnResource,
    Aws,
    CfnParameter,
    CfnMapping,
    CfnCondition,
)

from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression


@jsii.implements(IGrantable)
class PolicyFactory(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        data_bucket: Bucket,
        kms_key_arn: str,
        kms_enabled: CfnCondition,
    ):
        super().__init__(scope, id)
        self.kms_key_arn = kms_key_arn
        self.kms_enabled = kms_enabled
        self.kms_read_policy = self._kms_read_policy()
        self.kms_write_policy = self._kms_write_policy()
        self.forecast_service_ro_role = self._forecast_role(
            data_bucket, read=True, write=False
        )
        self.forecast_service_rw_role = self._forecast_role(
            data_bucket, read=True, write=True
        )

    def _kms_read_policy(self):
        policy = Policy(
            self,
            "ForecastKmsReadAccess",
            statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
                    actions=[
                        "kms:Decrypt",
                        "kms:DescribeKey",
                        "kms:GenerateDataKey",
                    ],
                    resources=[self.kms_key_arn],
                )
            ],
        )
        policy.node.default_child.cfn_options.condition = self.kms_enabled
        return policy

    def _kms_write_policy(self):
        policy = Policy(
            self,
            "ForecastKmsWriteAccess",
            statements=[
                PolicyStatement(
                    effect=Effect.ALLOW,
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
                    resources=[self.kms_key_arn],
                )
            ],
        )
        policy.node.default_child.cfn_options.condition = self.kms_enabled
        return policy

    def _forecast_role(self, data_bucket: Bucket, read=False, write=False):
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
                "forecast s3 role must have read, write, or both set to true"
            )

        role_id = f"ForecastS3{mode}Role"
        role = Role(
            self,
            role_id,
            assumed_by=ServicePrincipal("forecast.amazonaws.com"),
            inline_policies={
                role_id: PolicyDocument(
                    statements=[
                        PolicyStatement(
                            actions=actions,
                            resources=[
                                data_bucket.arn_for_objects("*"),
                                data_bucket.bucket_arn,
                            ],
                        )
                    ]
                )
            },
        )
        if read:
            self.kms_read_policy.attach_to_role(role)
        if write:
            self.kms_write_policy.attach_to_role(role)
        return role

    def _grant_forecast_access(self, grantee: IGrantable):
        grantee.grant_principal.add_to_principal_policy(
            PolicyStatement(
                actions=[
                    "forecast:Describe*",
                    "forecast:List*",
                    "forecast:Create*",
                    "forecast:Update*",
                    "forecast:TagResource",
                ],
                resources=["*"],
            )
        )

    def _grant_forecast_passrole(self, grantee: IGrantable, role: Role):
        grantee.grant_principal.add_to_principal_policy(
            PolicyStatement(actions=["iam:PassRole"], resources=[role.role_arn])
        )
        if isinstance(grantee, Function):
            grantee.add_environment("FORECAST_ROLE", role.role_arn)
            grantee.add_environment("FORECAST_KMS", self.kms_key_arn)

    def grant_forecast_read(self, grantee: IGrantable):
        self._grant_forecast_access(grantee)
        self._grant_forecast_passrole(grantee, self.forecast_service_ro_role)

    def grant_forecast_read_write(self, grantee: IGrantable):
        self._grant_forecast_access(grantee)
        self._grant_forecast_passrole(grantee, self.forecast_service_rw_role)

    def quicksight_access(
        self,
        grantee: IGrantable,
        catalog: CfnResource,
        workgroup: CfnResource,
        quicksight_principal: CfnParameter,
        quicksight_source: CfnMapping,
        athena_bucket: Bucket,
        data_bucket: Bucket,
    ):
        grantee.grant_principal.add_to_principal_policy(
            PolicyStatement(
                actions=[
                    "athena:StartQueryExecution",
                    "athena:GetQueryExecution",
                    "athena:GetQueryResults",
                ],
                resources=[
                    f"arn:{Aws.PARTITION}:athena:{Aws.REGION}:{Aws.ACCOUNT_ID}:workgroup/{workgroup.ref}"
                ],
            )
        )
        # grant glue access as well
        self.glue_access(
            grantee,
            catalog=catalog,
            athena_bucket=athena_bucket,
            data_bucket=data_bucket,
        )

        # grant quicksight access
        grantee.grant_principal.add_to_principal_policy(
            PolicyStatement(
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
                resources=[
                    "*",
                ],
            )
        )

        # set environment for the function
        if isinstance(grantee, Function):
            grantee.add_environment("SCHEMA_NAME", catalog.ref)
            grantee.add_environment("WORKGROUP_NAME", workgroup.ref)
            grantee.add_environment(
                "QUICKSIGHT_PRINCIPAL", quicksight_principal.value_as_string
            )
            grantee.add_environment(
                "QUICKSIGHT_SOURCE",
                quicksight_source.find_in_map("General", "QuickSightSourceTemplateArn"),
            )
            add_cfn_nag_suppressions(
                resource=grantee.role.node.children[1].node.default_child,
                suppressions=[
                    CfnNagSuppression(
                        "W76",
                        "Access to Glue, QuickSight and S3 require a large number of permissions",
                    ),
                    CfnNagSuppression(
                        "W12", "IAM policy for AWS X-Ray requires an allow on *"
                    ),
                ],
            )

    def glue_access(
        self,
        grantee: IGrantable,
        catalog: CfnResource,
        athena_bucket: Bucket,
        data_bucket: Bucket,
    ):
        grantee.grant_principal.add_to_principal_policy(
            PolicyStatement(
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
            )
        )

        grantee.grant_principal.add_to_principal_policy(
            PolicyStatement(
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
                    data_bucket.arn_for_objects("*"),
                    data_bucket.bucket_arn,
                ],
            )
        )
