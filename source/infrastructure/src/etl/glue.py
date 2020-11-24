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
from aws_cdk.core import Construct, Aws, CfnResource


class Glue(Construct):
    def __init__(
        self, scope: Construct, id: str, unique_name: CfnResource,
    ):
        super().__init__(scope, id)

        # implementation of CDK CfnDatabase is incomplete, use CfnResource
        self.database = CfnResource(
            self,
            "DataCatalog",
            type="AWS::Glue::Database",
            properties={
                "CatalogId": Aws.ACCOUNT_ID,
                "DatabaseInput": {
                    "Name": f"forecast_{unique_name.get_att('Id').to_string()}",
                    "Description": f"Database for Improving Forecast Accuracy with Machine Learning (stack: {Aws.STACK_NAME})",
                },
            },
        )
        self.database.override_logical_id("DataCatalog")

        self.security_configuration = CfnResource(
            self,
            "SecurityConfiguration",
            type="AWS::Glue::SecurityConfiguration",
            properties={
                "Name": f"Security Configuration for {Aws.STACK_NAME}",
                "EncryptionConfiguration": {
                    "CloudWatchEncryption": {
                        "CloudWatchEncryptionMode": "SSE-KMS",
                        "KmsKeyArn": f"arn:{Aws.PARTITION}:kms:{Aws.REGION}:{Aws.ACCOUNT_ID}/alias/aws/glue",
                    },
                    "JobBookmarksEncryption": {
                        "JobBookmarksEncryptionMode": "CSE-KMS",
                        "KmsKeyArn": f"arn:{Aws.PARTITION}:kms:{Aws.REGION}:{Aws.ACCOUNT_ID}/alias/aws/glue",
                    },
                    "S3Encryptions": [{"S3EncryptionMode": "SSE-S3"}],
                },
            },
        )
        self.security_configuration.override_logical_id("SecurityConfiguration")
