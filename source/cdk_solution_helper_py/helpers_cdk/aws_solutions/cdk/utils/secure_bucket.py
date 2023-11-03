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

import logging
from typing import List

import aws_cdk.aws_iam as iam
from constructs import Construct
from aws_cdk.aws_s3 import Bucket, BucketEncryption, BlockPublicAccess
from aws_cdk import RemovalPolicy, CfnResource, Aws

from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression

logger = logging.getLogger("cdk-helper")


class SecureBucket(Bucket):
    def __init__(
            self,
            scope: Construct,
            construct_id: str,
            suppress: List[CfnNagSuppression] = None,
            **kwargs,
    ):
        self.construct_id = construct_id

        kwargs = self.override_configuration(
            kwargs, "removal_policy", RemovalPolicy.RETAIN
        )
        kwargs = self.override_configuration(
            kwargs, "encryption", BucketEncryption.S3_MANAGED
        )
        kwargs = self.override_configuration(
            kwargs, "block_public_access", BlockPublicAccess.BLOCK_ALL
        )

        super().__init__(scope, construct_id, **kwargs)

        self.add_to_resource_policy(
            iam.PolicyStatement(
                sid="HttpsOnly",
                resources=[
                    self.arn_for_objects("*"),
                ],
                actions=["*"],
                effect=iam.Effect.DENY,
                principals=[iam.AnyPrincipal()],
                conditions={"Bool": {"aws:SecureTransport": False}},
            )
        )

        bucket_cfn: CfnResource = self.node.default_child
        bucket_cfn.override_logical_id(construct_id)
        if suppress:
            add_cfn_nag_suppressions(bucket_cfn, suppress)

        self.add_access_logs_bucket_policy(kwargs)

    def override_configuration(self, config, key, default=None):
        if not config.get(key):
            config[key] = default
        else:
            logger.warning(f"overriding {key} may reduce the security of the solution")
        return config

    def add_access_logs_bucket_policy(self, config):
        if config.get("server_access_logs_bucket"):
            access_logs_bucket = config.get("server_access_logs_bucket")

            # remove ACL
            access_logs_bucket.node.default_child.add_deletion_override(
                "Properties.AccessControl"
            )

            # add s3 bucket policy to write to access logging bucket
            access_logs_bucket.add_to_resource_policy(
                iam.PolicyStatement(
                    sid="S3ServerAccessLogsPolicy",
                    resources=[
                        f"{access_logs_bucket.bucket_arn}/*",
                    ],
                    actions=["s3:PutObject"],
                    effect=iam.Effect.ALLOW,
                    principals=[iam.ServicePrincipal("logging.s3.amazonaws.com")],
                    conditions={
                        "ArnLike": {
                            "aws:SourceArn": [
                                self.bucket_arn
                            ]
                        },
                        "StringEquals": {"aws:SourceAccount": Aws.ACCOUNT_ID}
                    },
                )
            )
