# #####################################################################################################################
#  Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                            #
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
from pathlib import Path

from aws_cdk.aws_s3 import Location, Bucket
from aws_cdk.core import Construct, Duration, CfnResource

from stepfunctions.lambda_builder import LambdaBuilder


def is_solution_build(construct: Construct):
    """Detect if this is being run from build-s3-dist.py and should package assets accordingly"""
    solutions_assets_regional = construct.node.try_get_context(
        "SOLUTIONS_ASSETS_REGIONAL"
    )
    solutions_assets_global = construct.node.try_get_context("SOLUTIONS_ASSETS_GLOBAL")
    return solutions_assets_regional and solutions_assets_global


class UrlDownloader(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        source: Location,
        destination: Location,
        lambda_log_level="INFO",
    ):
        super().__init__(scope, id)

        # name the function
        self.name = f"{id}UrlDownloader".replace("_", "")

        # functions and permissions
        downloader_function = self.url_builder_function(lambda_log_level)
        source_bucket = Bucket.from_bucket_attributes(
            self, f"{self.name}SourceBucket", bucket_name=source.bucket_name
        )
        destination_bucket = Bucket.from_bucket_attributes(
            self, f"{self.name}DestinationBucket", bucket_name=destination.bucket_name
        )
        source_bucket.grant_read(
            downloader_function, objects_key_pattern=source.object_key
        )
        destination_bucket.grant_put(
            downloader_function, objects_key_pattern=destination.object_key
        )

        # Custom resource to perform the download
        self.downloader = CfnResource(
            self,
            self.name,
            type="Custom::UrlDownloader",
            properties={
                "ServiceToken": downloader_function.function_arn,
                "SourceBucket": source.bucket_name,
                "SourceKey": source.object_key,
                "Scheme": "s3",
                "DestinationBucket": destination.bucket_name,
                "DestinationKey": destination.object_key,
            },
        )

    def url_builder_function(self, lambda_log_level="INFO"):
        lambda_builder = LambdaBuilder(
            self,
            log_level=lambda_log_level,
            source_path=Path(__file__).parent.parent.parent.parent,
        )
        functions = lambda_builder.functions_for(
            name="CfnResource",
            base="lambdas/cloudformation_resources",
            handlers=["url_downloader.handler"],
            timeout=Duration.seconds(30),
        )
        return functions["CfnResourceUrlDownloader"]
