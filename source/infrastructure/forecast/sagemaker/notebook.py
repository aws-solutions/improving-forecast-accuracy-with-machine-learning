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

import os
from pathlib import Path
from typing import List, Optional

import aws_cdk.aws_iam as iam
from aws_cdk.aws_s3 import IBucket
from aws_cdk.aws_s3_deployment import Source, BucketDeployment
from aws_cdk.aws_sagemaker import (
    CfnNotebookInstance,
    CfnNotebookInstanceLifecycleConfig,
)
from aws_cdk.core import Construct, CfnTag, Fn, Aws, CfnCondition, Aspects

from aws_solutions.cdk.aspects import ConditionalResources
from aws_solutions.cdk.cfn_nag import CfnNagSuppression, add_cfn_nag_suppressions
from aws_solutions.cdk.utils import is_solution_build
from forecast.sagemaker.policies import NotebookPolicies

context = '","'.join(
    [
        notebook.name
        for notebook in (
            Path(__file__).absolute().parents[3] / "notebook" / "samples" / "notebooks"
        ).glob("*.ipynb")
    ]
)


class Notebook(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        buckets: List[IBucket] = None,
        instance_type: str = "ml.t3.medium",
        instance_volume_size: int = 10,
        notebook_path: Optional[Path] = None,
        notebook_destination_bucket: IBucket = None,
        notebook_destination_prefix: str = None,
        create_notebook: Optional[CfnCondition] = None,
    ):  # NOSONAR
        super().__init__(scope, id)
        self.buckets = buckets if buckets else []
        self.deployment = None
        self.instance = None
        self.policies = NotebookPolicies(self)

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
                CfnTag(
                    key="NOTEBOOK_PREFIX",
                    value=self.get_notebook_prefix(),
                ),
            ],
        )
        self.instance.add_property_override("PlatformIdentifier", "notebook-al2-v1")
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

        Aspects.of(self).add(ConditionalResources(create_notebook))

    def get_notebook_prefix(self):
        if is_solution_build(self):
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
        if is_solution_build(self):
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
