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

from typing import List

import aws_cdk.aws_iam as iam
from aws_cdk.aws_s3 import IBucket
from aws_cdk.core import Fn, Aws, CfnResource


S3_ALL_ACTIONS = [
    "s3:GetObject",
    "s3:GetBucketLocation",
    "s3:ListBucket",
    "s3:ListObjects",
    "s3:ListBucketMultipartUploads",
    "s3:ListMultipartUploadParts",
    "s3:PutObject",
    "s3:AbortMultipartUpload",
    "s3:DeleteObject",
    "s3:CreateMultipartUpload",
]


class GluePolicies:
    def s3_read_write_access(self, buckets: List[IBucket]):
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=S3_ALL_ACTIONS,
                    resources=[bucket.arn_for_objects("*") for bucket in buckets]
                    + [bucket.bucket_arn for bucket in buckets],
                )
            ]
        )

    def s3_solutions_read_access(self):
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
                        f"arn:{Aws.PARTITION}:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws-glue/jobs/*"
                    ],
                )
            ]
        )

    def forecast_read(self):
        return iam.PolicyDocument(
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "forecast:Describe*",
                        "forecast:List*",
                    ],
                    resources=["*"],
                )
            ]
        )

    def glue_access(
        self,
        database: CfnResource,
        athena_bucket: IBucket,
        data_bucket: IBucket,
    ):
        return iam.PolicyDocument(
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
                        "glue:GetSecurityConfiguration",
                        "glue:GetSecurityConfigurations",
                        "glue:GetDataCatalogEncryptionSettings",
                    ],
                    resources=[
                        f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog",
                        f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:{database.ref}",
                        f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{database.ref}/*",
                        f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{database.ref}",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=S3_ALL_ACTIONS,
                    resources=[
                        athena_bucket.arn_for_objects("*"),
                        athena_bucket.bucket_arn,
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=S3_ALL_ACTIONS,
                    resources=[
                        data_bucket.arn_for_objects("*"),
                        data_bucket.bucket_arn,
                    ],
                ),
            ],
        )
