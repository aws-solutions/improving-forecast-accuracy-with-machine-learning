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
from dataclasses import field, dataclass
from typing import Optional

import aws_cdk.aws_iam as iam
from constructs import Construct
from aws_cdk.aws_s3 import Location
from aws_cdk import (
    CfnParameter,
    CfnCondition,
    Fn,
    Aws,
    Aspects,
)

from aws_solutions.cdk.aspects import ConditionalResources
from aws_solutions.cdk.aws_lambda.cfn_custom_resources.url_downloader import (
    UrlDownloader,
)
from aws_solutions.cdk.stack import NestedSolutionStack


@dataclass
class DownloaderParameterResource:
    parent: Construct
    name: str

    parameter: CfnParameter = field(init=False, repr=False)
    condition: CfnCondition = field(init=False, repr=False)

    def __post_init__(self):
        condition_name = f"{self.name}Provided"

        self.parameter = CfnParameter(self.parent, self.name, default="")
        self.condition = CfnCondition(
            self.parent,
            condition_name,
            expression=Fn.condition_not(Fn.condition_equals(self.parameter, "")),
        )


@dataclass
class DownloaderParameter:
    parent: Construct
    name: str
    forecast_name: CfnParameter
    destination: CfnParameter

    url: DownloaderParameterResource = field(default=None, repr=False, init=False)
    scheme: DownloaderParameterResource = field(default=None, repr=False, init=False)
    bucket: DownloaderParameterResource = field(default=None, repr=False, init=False)
    key: DownloaderParameterResource = field(default=None, repr=False, init=False)
    downloader: UrlDownloader = field(default=None, repr=False, init=False)

    def __post_init__(self):
        self.url = DownloaderParameterResource(self.parent, f"{self.name}Url")
        self.scheme = DownloaderParameterResource(self.parent, f"{self.name}Scheme")
        self.bucket = DownloaderParameterResource(self.parent, f"{self.name}Bucket")
        self.key = DownloaderParameterResource(self.parent, f"{self.name}Key")
        self.downloader = self._create_downloader()

    def _create_downloader(self):
        downloader = UrlDownloader(
            self.parent,
            f"{self.name}Downloader",
            destination=Location(
                bucket_name=self.destination.value_as_string,
                object_key=self.destination_key,
            ),
            source_url=self.url.parameter.value_as_string,
            source_bucket=self.bucket.parameter.value_as_string,
            source_key=self.key.parameter.value_as_string,
            scheme=self.scheme.parameter.value_as_string,
        )

        policy = iam.Policy(
            self.parent,
            f"{downloader.name}S3AccessPolicy",
            document=iam.PolicyDocument(
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["s3:GetObject"],
                        resources=[
                            f"arn:{Aws.PARTITION}:s3:::{self.bucket.parameter.value_as_string}/{self.key.parameter.value_as_string}"
                        ],
                    )
                ]
            ),
        )
        policy.attach_to_role(downloader.function.role)
        Aspects.of(policy).add(ConditionalResources(self.bucket.condition))

        return downloader

    @property
    def destination_key(self) -> str:
        if self.name == "ForecastDefaults":
            return "forecast-defaults.yaml"
        elif self.name == "TTS":
            return f"train/{self.forecast_name.value_as_string}.csv"
        elif self.name == "RTS":
            return f"train/{self.forecast_name.value_as_string}.related.csv"
        elif self.name == "MD":
            return f"train/{self.forecast_name.value_as_string}.metadata.csv"
        else:
            raise ValueError(f"invalid downloader name: {self.name}")


@dataclass
class Downloaders:
    """Track resources related to a file download/ transfer for Forecast demos"""

    parent: Construct
    forecast_name: CfnParameter
    destination: CfnParameter

    forecast_defaults: Optional[DownloaderParameter] = field(
        default=None, repr=False, init=False
    )
    tts: Optional[DownloaderParameter] = field(default=None, repr=False, init=False)
    rts: Optional[DownloaderParameter] = field(default=None, repr=False, init=False)
    md: Optional[DownloaderParameter] = field(default=None, repr=False, init=False)

    def __post_init__(self):
        self.forecast_defaults = DownloaderParameter(
            self.parent, "ForecastDefaults", self.forecast_name, self.destination
        )
        self.tts = DownloaderParameter(
            self.parent, "TTS", self.forecast_name, self.destination
        )
        self.rts = DownloaderParameter(
            self.parent, "RTS", self.forecast_name, self.destination
        )
        self.md = DownloaderParameter(
            self.parent, "MD", self.forecast_name, self.destination
        )

        self.tts.downloader.node.add_dependency(self.forecast_defaults.downloader)
        self.rts.downloader.node.add_dependency(self.forecast_defaults.downloader)
        self.md.downloader.node.add_dependency(self.forecast_defaults.downloader)


class Downloader(NestedSolutionStack):
    """This stack provides the means to demo Amazon Forecast using the NYC taxi dataset"""

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.destination_bucket = CfnParameter(self, id="DestinationBucket")
        self.forecast_name = CfnParameter(self, id="ForecastName")
        self.version = CfnParameter(self, id="Version")

        # build resources for downloading files
        Downloaders(self, self.forecast_name, self.destination_bucket)
