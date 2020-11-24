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

import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, Union

import aws_cdk.aws_iam as iam
from aws_cdk.aws_s3 import IBucket
from aws_cdk.aws_s3_deployment import Source, BucketDeployment
from aws_cdk.aws_sagemaker import (
    CfnNotebookInstance,
    CfnNotebookInstanceLifecycleConfig,
)
from aws_cdk.core import Construct, CfnTag, Fn, Aws

from solutions.cfn_nag import CfnNagSuppression, add_cfn_nag_suppressions


@dataclass
class NotebookInlinePolicies:
    owner: Construct

    def s3_access(self, buckets: List[IBucket]):
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetObject",
                        "s3:GetBucketLocation",
                        "s3:ListBucket",
                        "s3:ListObjects",
                        "s3:ListBucketMultipartUploads",
                        "s3:ListMultipartUploadParts",
                        "s3:PutObject",
                        "s3:AbortMultipartUpload",
                        "s3:DeleteObject",
                    ],
                    resources=[bucket.arn_for_objects("*") for bucket in buckets]
                    + [bucket.bucket_arn for bucket in buckets],
                )
            ]
        )

    def s3_solutions_access(self):
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["s3:GetObject", "s3:ListBucket", "s3:ListObjects",],
                    resources=[
                        Fn.sub(
                            "arn:${AWS::Partition}:s3:::${bucket}-${AWS::Region}/*",
                            variables={
                                "bucket": Fn.find_in_map(
                                    "SourceCode", "General", "S3Bucket"
                                )
                            },
                        ),
                        Fn.sub(
                            "arn:${AWS::Partition}:s3:::${bucket}-${AWS::Region}",
                            variables={
                                "bucket": Fn.find_in_map(
                                    "SourceCode", "General", "S3Bucket"
                                )
                            },
                        ),
                    ],
                )
            ]
        )

    def cloudwatch_logs_write(self):
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "logs:CreateLogGroup",
                        "logs:CreateLogStream",
                        "logs:PutLogEvents",
                    ],
                    resources=[
                        f"arn:{Aws.PARTITION}:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/sagemaker/*"
                    ],
                )
            ]
        )

    def sagemaker_tags_read(self):
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["sagemaker:ListTags"],
                    resources=[
                        f"arn:{Aws.PARTITION}:sagemaker:{Aws.REGION}:{Aws.ACCOUNT_ID}:notebook-instance/*-aws-forecast-visualization"
                    ],
                )
            ]
        )


class Notebook(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        buckets: List[IBucket] = None,
        instance_type: str = "ml.t2.medium",
        instance_volume_size: int = 10,
        notebook_path: Union[Path, None] = None,
        notebook_destination_bucket: IBucket = None,
        notebook_destination_prefix: str = None,
    ):
        super().__init__(scope, id)
        self.buckets = buckets if buckets else []
        self.deployment = None
        self.instance = None
        self.policies = NotebookInlinePolicies(self)

        # permissions for the notebook instance
        notebook_role = iam.Role(
            self,
            "InstanceRole",
            assumed_by=iam.ServicePrincipal("sagemaker.amazonaws.com"),
            inline_policies={
                "SagemakerNotebookCloudWatchLogs": self.policies.cloudwatch_logs_write(),
                "ForecastBucketAccessPolicy": self.policies.s3_access(buckets),
                "SagemakerNotebookListTags": self.policies.sagemaker_tags_read(),
                "NotebookBucketAccessPolicy": self.policies.s3_solutions_access(),
            },
        )

        # lifecycle configuration
        lifecycle_config_path = os.path.join(
            os.path.dirname(__file__), "lifecycle_config.py"
        )
        with open(lifecycle_config_path) as lifecycle_config:
            lifecycle_config_code = lifecycle_config.read()

        lifecycle_config = CfnNotebookInstanceLifecycleConfig(self, "LifecycleConfig")
        lifecycle_config.add_property_override(
            "OnStart", [{"Content": {"Fn::Base64": lifecycle_config_code}}]
        )

        # notebook instance
        self.instance = CfnNotebookInstance(
            self,
            "NotebookInstance",
            notebook_instance_name=f"{Aws.STACK_NAME}-aws-forecast-visualization",
            instance_type=instance_type,
            role_arn=notebook_role.role_arn,
            volume_size_in_gb=instance_volume_size,
            lifecycle_config_name=lifecycle_config.attr_notebook_instance_lifecycle_config_name,
            tags=[
                CfnTag(
                    key="FORECAST_BUCKET",
                    value=Fn.base64(notebook_destination_bucket.bucket_name),
                ),
                CfnTag(
                    key="NOTEBOOK_BUCKET",
                    value=self.get_notebook_source(notebook_destination_bucket),
                ),
                CfnTag(key="NOTEBOOK_PREFIX", value=self.get_notebook_prefix(),),
            ],
        )
        add_cfn_nag_suppressions(
            self.instance,
            [
                CfnNagSuppression(
                    "W1201",
                    "Require access to all resources; Not all Amazon Forecast resources support resource based policy",
                )
            ],
        )
        self.instance.override_logical_id("NotebookInstance")

        # create notebook assets
        if (
            notebook_path
            and notebook_destination_prefix
            and notebook_destination_bucket
        ):
            assets = [Source.asset(path=str(notebook_path))]
            self.deployment = BucketDeployment(
                self,
                "Notebooks",
                destination_bucket=notebook_destination_bucket,
                destination_key_prefix=notebook_destination_prefix,
                sources=assets,
            )

    def _is_solution_build(self):
        solutions_assets_regional = self.node.try_get_context(
            "SOLUTIONS_ASSETS_REGIONAL"
        )
        solutions_assets_global = self.node.try_get_context("SOLUTIONS_ASSETS_GLOBAL")
        return solutions_assets_regional and solutions_assets_global

    def get_notebook_prefix(self):
        if self._is_solution_build():
            prefix = Fn.sub(
                "${prefix}/notebooks",
                variables={
                    "prefix": Fn.find_in_map("SourceCode", "General", "KeyPrefix")
                },
            )
        else:
            prefix = "notebooks"
        return Fn.base64(prefix)

    def get_notebook_source(self, data_bucket: IBucket):
        if self._is_solution_build():
            notebook_source_bucket = Fn.sub(
                "${bucket}-${region}",
                variables={
                    "bucket": Fn.find_in_map("SourceCode", "General", "S3Bucket"),
                    "region": Aws.REGION,
                },
            )

        else:
            notebook_source_bucket = data_bucket.bucket_name

        return Fn.base64(notebook_source_bucket)
