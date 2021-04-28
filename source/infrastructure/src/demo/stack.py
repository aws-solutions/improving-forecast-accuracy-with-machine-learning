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
from typing import Dict

from aws_cdk.aws_lambda import Function
from aws_cdk.core import (
    Stack,
    CfnParameter,
    App,
    CfnCondition,
    Fn,
    CfnResource,
    Aspects,
    Duration,
    Aws,
    RemovalPolicy,
)

from demo.downloader import DemoDownloader
from demo.policies import DemoPolicies
from forecast.parameters import Parameters
from forecast.stack import NestedForecastStack
from interfaces import TemplateOptions, ConditionalResources
from interfaces.CfnNagSuppressAll import CfnNagSuppressAll
from solutions.cfn_nag import CfnNagSuppression
from solutions.mappings import Mappings
from solutions.metrics import Metrics
from stepfunctions.lambda_builder import LambdaBuilder

# These are the defaults for the CloudFormation parameters
FORECAST_NAME_DEFAULT = "nyctaxi_weather_auto"
FORECAST_CONFIG_DEFAULT = "s3://amazon-forecast-samples/automation_solution/demo-nyctaxi/forecast-defaults.yaml"
TTS_URL_DEFAULT = "s3://amazon-forecast-samples/automation_solution/demo-nyctaxi/nyctaxi_weather_auto.csv"
RTS_URL_DEFAULT = "s3://amazon-forecast-samples/automation_solution/demo-nyctaxi/nyctaxi_weather_auto.related.csv"
MD_URL_DEFAULT = "s3://amazon-forecast-samples/automation_solution/demo-nyctaxi/nyctaxi_weather_auto.metadata.csv"
FORECAST_STACK_DEFAULT = ""
CFN_RESOURCE_TYPE_URL_INFO = "Custom::UrlInfo"


class DemoStack(Stack):
    def __init__(self, app: App, id: str, **kwargs) -> None:
        super().__init__(app, id, **kwargs)

        template_options = TemplateOptions(
            self,
            id=id,
            description="(SO0123td) Improving Forecast Accuracy with Machine Learning %%VERSION%% - NYC Taxi Data Demo",
            filename="improving-forecast-accuracy-with-machine-learning-demo.template",
        )
        self.mappings = Mappings(self, "SO0123td")
        self.policies = DemoPolicies(self)

        re_http_https_s3 = (
            r"^https?://([-\w\.]+)+(:\d+)?(/([-\w/_\.]*(\?\S+)?)?)?$|^s3://(.*)/(.*)$"
        )
        re_http_https_s3_or_blank = r"^https?://([-\w\.]+)+(:\d+)?(/([-\w/_\.]*(\?\S+)?)?)?$|^s3://(.*)/(.*)$|^$"

        forecast_name = CfnParameter(
            self,
            id="ForecastName",
            type="String",
            description="Name of the forecast to create in the Amazon Forecast console",
            default=FORECAST_NAME_DEFAULT,
            allowed_pattern=r"^[a-zA-Z][a-zA-Z0-9_]*$",
            max_length=63,
            constraint_description="Forecast names must be less than 63 characters, start with a letter, and contain only alphanumeric characters or underscores",
        )

        forecast_defaults_url = CfnParameter(
            self,
            id="ForecastDefaultsUrl",
            type="String",
            description="URL (S3, HTTP or HTTPS) your forecast defaults file (usually named forecast-defaults.yaml)",
            allowed_pattern=re_http_https_s3,
            default=FORECAST_CONFIG_DEFAULT,
            constraint_description="Must be a valid s3/http/https address",
        )

        tts_url = CfnParameter(
            self,
            id="TargetTimeSeriesUrl",
            type="String",
            description="URL (S3, HTTP or HTTPS) for target time series data",
            allowed_pattern=re_http_https_s3,
            default=TTS_URL_DEFAULT,
            constraint_description="Must be a valid s3/http/https address",
        )

        rts_url = CfnParameter(
            self,
            id="RelatedTimeSeriesUrl",
            type="String",
            description="URL (S3, HTTP or HTTPS) for related time series data",
            allowed_pattern=re_http_https_s3_or_blank,
            default=RTS_URL_DEFAULT,
            constraint_description="Must be a valid s3/http/https address or blank",
        )

        md_url = CfnParameter(
            self,
            id="MetadataUrl",
            type="String",
            description="URL (S3, HTTP or HTTPS) for item metadata",
            allowed_pattern=re_http_https_s3_or_blank,
            default=MD_URL_DEFAULT,
            constraint_description="Must be a valid http/https address or blank",
        )

        forecast_stack_name = CfnParameter(
            self,
            id="ForecastStackName",
            type="String",
            description="Existing forecast stack name",
            default=FORECAST_STACK_DEFAULT,
            constraint_description="Must be a valid CloudFormation stack name or blank (if blank, this will deploy a new stack)",
        )

        # Conditions
        self.defaults_provided = CfnCondition(
            self,
            "ForecastDefaultsProvided",
            expression=Fn.condition_not(Fn.condition_equals(forecast_defaults_url, "")),
        )

        self.tts_provided = CfnCondition(
            self,
            "TTSProvided",
            expression=Fn.condition_not(Fn.condition_equals(tts_url, "")),
        )

        self.rts_provided = CfnCondition(
            self,
            "RTSProvided",
            expression=Fn.condition_not(Fn.condition_equals(rts_url, "")),
        )

        self.md_provided = CfnCondition(
            self,
            "MDProvided",
            expression=Fn.condition_not(Fn.condition_equals(md_url, "")),
        )

        self.stack_provided = CfnCondition(
            self,
            "ForecastStackNameProvided",
            expression=Fn.condition_not(Fn.condition_equals(forecast_stack_name, "")),
        )

        self.stack_not_provided = CfnCondition(
            self,
            "ForecastStackNameNotProvided",
            expression=Fn.condition_equals(forecast_stack_name, ""),
        )

        # Parameter Groups:
        template_options.add_parameter_group(
            label="Forecast Configuration",
            parameters=[forecast_name, forecast_defaults_url],
        )
        template_options.add_parameter_group(
            label="Datasets Configuration",
            parameters=[tts_url, rts_url, md_url],
        )
        template_options.add_parameter_group(
            label="Forecast Stack (Optional)",
            parameters=[forecast_stack_name],
        )
        child_parameters = Parameters(self, limited_parameters=True)

        # Parameter Labels:
        template_options.add_parameter_label(tts_url, "Target Time Series URL")
        template_options.add_parameter_label(
            rts_url, "Related Time Series URL (or blank)"
        )
        template_options.add_parameter_label(md_url, "Item Metadata URL (or blank)")
        template_options.add_parameter_label(
            forecast_defaults_url, "Forecast Defaults URL (or blank)"
        )
        template_options.add_parameter_label(
            forecast_stack_name,
            "If provided, use an existing Improving Forecast Accuracy with Machine Learning stack",
        )
        template_options.add_parameter_label(
            child_parameters.lambda_log_level, "CloudWatch Log Level"
        )
        child_parameters.add_template_options(template_options)

        # Build Required Lambda Functions
        functions: Dict[str, Function] = {}
        lambda_builder = LambdaBuilder(
            self,
            log_level=child_parameters.lambda_log_level.value_as_string,
            source_path=Path(__file__).parent.parent.parent.parent,
        )
        functions.update(
            lambda_builder.functions_for(
                name="CfnResource",
                base="lambdas/cloudformation_resources",
                handlers=[
                    "url_helper.handler",
                    "stack_outputs.handler",
                    "solution_metrics.handler",
                ],
                timeout=Duration.seconds(30),
            )
        )

        # stack outputs for forecast bucket
        functions["CfnResourceStackOutputs"].role.add_to_policy(
            self.policies.cloudformation_read(forecast_stack_name.value_as_string)
        )
        Aspects.of(functions["CfnResourceStackOutputs"]).add(
            ConditionalResources(self.stack_provided)
        )

        self.forecast_defaults_url_info = CfnResource(
            self,
            "ForecastDefaultsUrlInfo",
            type=CFN_RESOURCE_TYPE_URL_INFO,
            properties={
                "ServiceToken": functions["CfnResourceUrlHelper"].function_arn,
                "Url": forecast_defaults_url.value_as_string,
            },
        )

        self.tts_url_info = CfnResource(
            self,
            "TTSUrlInfo",
            type=CFN_RESOURCE_TYPE_URL_INFO,
            properties={
                "ServiceToken": functions["CfnResourceUrlHelper"].function_arn,
                "Url": tts_url.value_as_string,
            },
        )

        self.rts_url_info = CfnResource(
            self,
            "RTSUrlInfo",
            type=CFN_RESOURCE_TYPE_URL_INFO,
            properties={
                "ServiceToken": functions["CfnResourceUrlHelper"].function_arn,
                "Url": rts_url.value_as_string,
            },
        )
        Aspects.of(self.rts_url_info).add(ConditionalResources(self.rts_provided))

        self.md_url_info = CfnResource(
            self,
            "MDUrlInfo",
            type=CFN_RESOURCE_TYPE_URL_INFO,
            properties={
                "ServiceToken": functions["CfnResourceUrlHelper"].function_arn,
                "Url": md_url.value_as_string,
            },
        )
        Aspects.of(self.md_url_info).add(ConditionalResources(self.md_provided))

        # synthesize the main stack
        forecast_stack_cdk = NestedForecastStack(
            self, "ForecastStack", parameters=child_parameters.for_deployment()
        )

        forecast_stack_cdk.nested_stack_resource.cfn_options.condition = (
            self.stack_not_provided
        )
        forecast_stack_cdk.nested_stack_resource.apply_removal_policy(
            RemovalPolicy.RETAIN
        )
        forecast_stack_cdk.nested_stack_resource.override_logical_id("ForecastStack")

        # create or read information about an existing stack
        cfn_stack_info = CfnResource(
            self,
            "ForecastStackInfo",
            type="Custom::StackInfo",
            properties={
                "ServiceToken": functions["CfnResourceStackOutputs"].function_arn,
                "Stack": Fn.condition_if(
                    "ForecastStackNameProvided",
                    forecast_stack_name.value_as_string,
                    Aws.NO_VALUE,
                ).to_string(),
            },
        )
        Aspects.of(cfn_stack_info).add(ConditionalResources(self.stack_provided))

        # prepare the nested stack that performs the actual downloads
        downloader = DemoDownloader(
            self,
            "DemoDownloader",
            parameters={
                "DestinationBucket": Fn.condition_if(
                    "ForecastStackNameProvided",
                    cfn_stack_info.get_att("ForecastBucketName").to_string(),
                    Fn.condition_if(
                        "ForecastStackNameNotProvided",
                        forecast_stack_cdk.nested_stack_resource.get_att(
                            "Outputs.ForecastBucketName"
                        ).to_string(),
                        Aws.NO_VALUE,
                    ),
                ).to_string(),
                "ForecastName": forecast_name.value_as_string,
                "LambdaLogLevel": child_parameters.lambda_log_level.value_as_string,
                **{
                    f"{file}{info}": Fn.condition_if(
                        f"{file}Provided",
                        Fn.get_att(f"{file}UrlInfo", f"{info}").to_string(),
                        "",
                    ).to_string()
                    for file in ["TTS", "RTS", "MD", "ForecastDefaults"]
                    for info in ["Url", "Scheme", "Bucket", "Key"]
                },
            },
        )
        downloader.nested_stack_resource.override_logical_id("DemoDownloader")

        self.metrics = Metrics(
            self,
            "SolutionMetrics",
            metrics_function=functions["CfnResourceSolutionMetrics"],
            metrics={
                "Solution": self.mappings.solution_mapping.find_in_map("Data", "ID"),
                "Version": self.mappings.solution_mapping.find_in_map(
                    "Data", "Version"
                ),
                "Region": Aws.REGION,
            },
        )

        # aspects
        Aspects.of(self).add(
            CfnNagSuppressAll(
                suppress=[
                    CfnNagSuppression(
                        "W89", "Solution AWS Lambda Functions are not deployed to a VPC"
                    ),
                    CfnNagSuppression(
                        "W92",
                        "Solution AWS Lambda Functions do not require reserved concurrency",
                    ),
                ],
                resource_type="AWS::Lambda::Function",
            )
        )
