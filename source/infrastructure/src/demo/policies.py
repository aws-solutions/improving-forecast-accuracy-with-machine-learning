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
import aws_cdk.aws_iam as iam
from aws_cdk.core import Aspects, CfnParameter, Aws

from interfaces import ConditionalResources
from policies import Policies


class DemoPolicies(Policies):
    def s3_access(self, downloader):
        policy = iam.Policy(
            self.owner,
            f"{downloader.name}S3AccessPolicy",
            document=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["s3:GetObject"],
                        resources=[
                            f"arn:{Aws.PARTITION}:s3:::{downloader.bucket.value_as_string}/{downloader.key.value_as_string}"
                        ],
                    )
                ]
            ),
        )
        Aspects.of(policy).add(ConditionalResources(downloader.bucket_provided))
        return policy

    def s3_destination_access(self, destination: CfnParameter):
        return iam.Policy(
            self.owner,
            "S3DestinationAccess",
            document=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["s3:PutObject"],
                        resources=[
                            f"arn:{Aws.PARTITION}:s3:::{destination.value_as_string}/forecast-defaults.yaml",
                            f"arn:{Aws.PARTITION}:s3:::{destination.value_as_string}/train/*",
                        ],
                    )
                ]
            ),
        )

    def cloudformation_read(self, template_name):
        return iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=["cloudformation:DescribeStacks"],
            resources=[
                f"arn:{Aws.PARTITION}:cloudformation:{Aws.REGION}:{Aws.ACCOUNT_ID}:stack/{template_name}/*"
            ],
        )
