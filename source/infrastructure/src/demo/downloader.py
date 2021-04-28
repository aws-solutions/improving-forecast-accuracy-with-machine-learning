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
from pathlib import Path
from typing import Union, Dict

from aws_cdk.aws_lambda import Function
from aws_cdk.core import (
    CfnResource,
    CfnParameter,
    CfnCondition,
    NestedStack,
    Construct,
    Duration,
    Fn,
    Aspects,
    Aws,
)

from demo.policies import DemoPolicies
from interfaces import TemplateOptions, ConditionalResources
from solutions.mappings import Mappings
from stepfunctions.lambda_builder import LambdaBuilder


@dataclass
class Downloader:
    """Track resources related to a file download/ transfer for Forecast demos"""

    name: str
    custom_resource: Union[CfnResource, None] = field(default=None, repr=False)

    scheme: Union[CfnParameter, None] = field(default=None)
    url: Union[CfnParameter, None] = field(default=None)
    bucket: Union[CfnParameter, None] = field(default=None, repr=False)
    key: Union[CfnParameter, None] = field(default=None, repr=False)

    url_provided: Union[CfnCondition, None] = field(default=None, repr=False)
    scheme_provided: Union[CfnCondition, None] = field(default=None, repr=False)
    bucket_provided: Union[CfnCondition, None] = field(default=None, repr=False)
    key_provided: Union[CfnCondition, None] = field(default=None, repr=False)


class DemoDownloader(NestedStack):
    """This stack provides the means to demo Amazon Forecast using the NYC taxi dataset"""

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self._template_options = TemplateOptions(
            self,
            id=id,
            description="(SO0123dc) Improving Forecast Accuracy with Machine Learning %%VERSION%% - NYC Taxi Data Demo (Data Copier)",
            filename="improving-forecast-accuracy-with-machine-learning-demo-downloader.template",
        )
        self.mappings = Mappings(self, "SO0123dc")
        policies = DemoPolicies(self)

        self.forecast_name = CfnParameter(self, id="ForecastName")
        self.destination_bucket = CfnParameter(self, id="DestinationBucket")
        lambda_log_level = CfnParameter(self, id="LambdaLogLevel", default="DEBUG")

        # build custom resource for downloading files
        functions: Dict[str, Function] = {}
        lambda_builder = LambdaBuilder(
            self,
            log_level=lambda_log_level.value_as_string,
            source_path=Path(__file__).parent.parent.parent.parent,
        )
        functions.update(
            lambda_builder.functions_for(
                name="CfnResource",
                base="lambdas/cloudformation_resources",
                handlers=["url_downloader.handler"],
                timeout=Duration.seconds(30),
            )
        )
        destination_access_policy = policies.s3_destination_access(
            self.destination_bucket
        )
        destination_access_policy.attach_to_role(
            functions["CfnResourceUrlDownloader"].role
        )

        # Build parameters, conditions and custom resources for all files passed in
        downloaders = {}
        for item in ["Forecast_Defaults", "TTS", "RTS", "MD"]:
            downloaders[item] = Downloader(item)
            for subitem in ["Url", "Scheme", "Bucket", "Key"]:
                parameter_name = f"{item.replace('_', '')}{subitem}"
                condition_name = f"{parameter_name}Provided"
                parameter = CfnParameter(self, id=parameter_name, default="")
                setattr(downloaders[item], subitem.lower(), parameter)
                if subitem == "Url" or subitem == "Bucket":
                    parameter_provided = CfnCondition(
                        self,
                        condition_name,
                        expression=Fn.condition_not(Fn.condition_equals(parameter, "")),
                    )
                    setattr(
                        downloaders[item],
                        f"{subitem.lower()}_provided",
                        parameter_provided,
                    )

            # add policy for this downloader
            downloader_policy = policies.s3_access(downloaders[item])
            downloader_policy.attach_to_role(functions["CfnResourceUrlDownloader"].role)
            Aspects.of(downloader_policy).add(
                ConditionalResources(downloaders[item].bucket_provided)
            )

            # custom resource used to perform the download
            downloaders[item].custom_resource = CfnResource(
                self,
                f"{item}UrlDownloader".replace("_", ""),
                type="Custom::UrlDownloader",
                properties={
                    "ServiceToken": functions["CfnResourceUrlDownloader"].function_arn,
                    "SourceUrl": downloaders[item].url,
                    "SourceBucket": downloaders[item].bucket.value_as_string,
                    "SourceKey": downloaders[item].key.value_as_string,
                    "Scheme": downloaders[item].scheme.value_as_string,
                    "DestinationBucket": self.destination_bucket.value_as_string,
                    "DestinationKey": self._build_destination_key(downloaders[item]),
                    "Policy": Fn.condition_if(
                        f"{item.replace('_', '')}BucketProvided",
                        downloader_policy.node.default_child.ref,
                        Aws.NO_VALUE,
                    ),
                },
            )
            downloaders[item].custom_resource.add_depends_on(
                destination_access_policy.node.default_child
            )
            if item in ["TTS", "RTS", "MD"]:
                downloaders[item].custom_resource.add_depends_on(
                    downloaders["Forecast_Defaults"].custom_resource
                )
                Aspects.of(downloaders[item].custom_resource).add(
                    ConditionalResources(downloaders[item].url_provided)
                )

    def _build_destination_key(self, downloader: Downloader) -> str:
        if downloader.name == "Forecast_Defaults":
            return "forecast-defaults.yaml"
        elif downloader.name == "TTS":
            return f"train/{self.forecast_name.value_as_string}.csv"
        elif downloader.name == "RTS":
            return f"train/{self.forecast_name.value_as_string}.related.csv"
        elif downloader.name == "MD":
            return f"train/{self.forecast_name.value_as_string}.metadata.csv"
        else:
            raise ValueError(f"invalid downloader name: {downloader.name}")
