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

from dataclasses import dataclass
from typing import List

import aws_cdk.aws_iam as iam
from aws_cdk.aws_s3 import IBucket
from aws_cdk.core import Construct, Fn, Aws


@dataclass
class NotebookPolicies:
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
                    actions=[
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:ListObjects",
                    ],
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
