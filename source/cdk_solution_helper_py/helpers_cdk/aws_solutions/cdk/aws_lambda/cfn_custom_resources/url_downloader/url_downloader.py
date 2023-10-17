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
from pathlib import Path
from typing import Optional

import aws_cdk.aws_iam as iam
from constructs import Construct
from aws_cdk.aws_s3 import Location, Bucket
from aws_cdk import CfnResource, Duration, Aws
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction


def downloader_s3_source_access(grantee: iam.IGrantable, bucket: str, key: str):
    grantee.grant_principal.add_to_principal_policy(
        iam.PolicyStatement(
            actions=["s3:GetObject"],
            resources=[f"arn:{Aws.PARTITION}:s3:::{bucket}/{key}"],
        )
    )


class UrlDownloader(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        destination: Location,
        source: Optional[Location] = None,
        source_url: Optional[str] = None,
        source_bucket: Optional[str] = None,
        source_key: Optional[str] = None,
        scheme: Optional[str] = None,
    ):
        super().__init__(scope, id)

        # name the function
        self.name = f"{id}UrlDownloader".replace("_", "")
        properties = {}

        # functions and permissions
        self.function = self.url_builder_function()
        if source:
            source_bucket = Bucket.from_bucket_attributes(
                self, f"{self.name}SourceBucket", bucket_name=source.bucket_name
            )
            source_bucket.grant_read(
                self.function, objects_key_pattern=source.object_key
            )
            properties.update(
                {
                    "SourceBucket": source.bucket_name,
                    "SourceKey": source.object_key,
                    "Scheme": "s3",
                }
            )
        elif source_url and source_bucket and scheme:

            properties.update(
                {
                    "SourceUrl": source_url,
                    "SourceBucket": source_bucket,
                    "SourceKey": source_key,
                    "Scheme": scheme,
                }
            )
        else:
            raise ValueError(
                "either source or all of source_url, source_bucket, source_key and scheme must be provided"
            )

        destination_bucket = Bucket.from_bucket_attributes(
            self, f"{self.name}DestinationBucket", bucket_name=destination.bucket_name
        )
        destination_bucket.grant_put(
            self.function, objects_key_pattern=destination.object_key
        )

        # Custom resource to perform the download
        self.downloader = CfnResource(
            self,
            self.name,
            type="Custom::UrlDownloader",
            properties={
                "ServiceToken": self.function.function_arn,
                "DestinationBucket": destination.bucket_name,
                "DestinationKey": destination.object_key,
                **properties,
            },
        )

    def url_builder_function(self):
        return SolutionsPythonFunction(
            self,
            self.name + "Function",
            entrypoint=Path(__file__).parent
            / "src"
            / "custom_resources"
            / "url_downloader.py",
            function="handler",
            timeout=Duration.seconds(300),
        )
