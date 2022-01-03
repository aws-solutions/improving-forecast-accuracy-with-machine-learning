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

from aws_cdk.core import CfnParameter, Construct

from aws_solutions.cdk import SolutionStack

logger = logging.getLogger("cdk-helper")


FORECAST_NAME_DEFAULT = "nyctaxi_weather_auto"
FORECAST_CONFIG_DEFAULT = "s3://amazon-forecast-samples/automation_solution/demo-nyctaxi/forecast-defaults.yaml"
TTS_URL_DEFAULT = "s3://amazon-forecast-samples/automation_solution/demo-nyctaxi/nyctaxi_weather_auto.csv"
RTS_URL_DEFAULT = "s3://amazon-forecast-samples/automation_solution/demo-nyctaxi/nyctaxi_weather_auto.related.csv"
MD_URL_DEFAULT = "s3://amazon-forecast-samples/automation_solution/demo-nyctaxi/nyctaxi_weather_auto.metadata.csv"
CFN_RESOURCE_TYPE_URL_INFO = "Custom::UrlInfo"


class ParameterSection:
    visualization_config = "Visualization Options"
    security_config = "Security Configuration"
    deployment_config = "Deployment Configuration"
    forecast_config = "Forecast Configuration"
    dataset_config = "Dataset Configuration"
    notification_configuration = "Notification Configuration"


class Parameters(Construct):
    def __init__(self, scope: SolutionStack, id: str):
        super().__init__(scope, id)

        re_http_https_s3 = (
            r"^https?://([-\w\.]+)+(:\d+)?(/([-\w/_\.]*(\?\S+)?)?)?$|^s3://(.*)/(.*)$"
        )
        re_http_https_s3_or_blank = r"^https?://([-\w\.]+)+(:\d+)?(/([-\w/_\.]*(\?\S+)?)?)?$|^s3://(.*)/(.*)$|^$"

        self.email = CfnParameter(
            scope,
            id="Email",
            type="String",
            description="Email to notify with forecast results",
            default="",
            max_length=50,
            allowed_pattern=r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$|^$)",
            constraint_description="Must be a valid email address or blank",
        )

        self.lambda_log_level = CfnParameter(
            scope,
            id="LambdaLogLevel",
            type="String",
            description="Change the verbosity of the logs output to CloudWatch",
            default="WARNING",
            allowed_values=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        )

        self.notebook_deploy = CfnParameter(
            scope,
            id="NotebookDeploy",
            type="String",
            description="Deploy an Amazon SageMaker Jupyter Notebook instance",
            default="No",
            allowed_values=["Yes", "No"],
        )

        self.notebook_volume_size = CfnParameter(
            scope,
            id="NotebookVolumeSize",
            type="Number",
            description="Enter the size of the notebook instance EBS volume in GB",
            default=10,
            min_value=5,
            max_value=16384,
            constraint_description="Must be an integer between 5 (GB) and 16384 (16 TB)",
        )

        self.notebook_instance_type = CfnParameter(
            scope,
            id="NotebookInstanceType",
            type="String",
            description="Enter the type of the notebook instance",
            default="ml.t3.medium",
            allowed_values=[
                "ml.t2.medium",
                "ml.t3.medium",
                "ml.r5.large",
                "ml.c5.large",
            ],
        )

        self.quicksight_analysis_owner = CfnParameter(
            scope,
            id="QuickSightAnalysisOwner",
            description="With QuickSight Enterprise enabled, provide a QuickSight ADMIN user ARN to automatically create QuickSight analyses",
            default="",
            allowed_pattern="(^arn:.*:quicksight:.*:.*:user.*$|^$)",
        )

        self.forecast_kms_key_arn = CfnParameter(
            scope,
            id="ForecastKmsKeyArn",
            description="Provide Amazon Forecast with an alternate AWS Key Management (KMS) key to use for CreatePredictor and CreateDataset operations",
            default="",
            allowed_pattern="(^arn:.*:kms:.*:.*:key/.*$|^$)",
        )

        # Downloader / Demo Configuration
        self.forecast_deploy = CfnParameter(
            scope,
            id="ForecastDeploy",
            type="String",
            description="Download and deploy these assets with the stack",
            default="No",
            allowed_values=["Yes", "No"],
        )
        self.forecast_name = CfnParameter(
            self,
            id="ForecastName",
            type="String",
            description="Name of the forecast to create in the Amazon Forecast console",
            default=FORECAST_NAME_DEFAULT,
            allowed_pattern=r"^[a-zA-Z][a-zA-Z0-9_]*$",
            max_length=63,
            constraint_description="Forecast names must be less than 63 characters, start with a letter, and contain only alphanumeric characters or underscores",
        )

        self.forecast_defaults_url = CfnParameter(
            self,
            id="ForecastDefaultsUrl",
            type="String",
            description="URL (S3, HTTP or HTTPS) your forecast defaults file (usually named forecast-defaults.yaml)",
            allowed_pattern=re_http_https_s3,
            default=FORECAST_CONFIG_DEFAULT,
            constraint_description="Must be a valid s3/http/https address",
        )

        self.tts_url = CfnParameter(
            self,
            id="TargetTimeSeriesUrl",
            type="String",
            description="URL (S3, HTTP or HTTPS) for target time series data",
            allowed_pattern=re_http_https_s3,
            default=TTS_URL_DEFAULT,
            constraint_description="Must be a valid s3/http/https address",
        )

        self.rts_url = CfnParameter(
            self,
            id="RelatedTimeSeriesUrl",
            type="String",
            description="URL (S3, HTTP or HTTPS) for related time series data",
            allowed_pattern=re_http_https_s3_or_blank,
            default=RTS_URL_DEFAULT,
            constraint_description="Must be a valid s3/http/https address or blank",
        )

        self.md_url = CfnParameter(
            self,
            id="MetadataUrl",
            type="String",
            description="URL (S3, HTTP or HTTPS) for item metadata",
            allowed_pattern=re_http_https_s3_or_blank,
            default=MD_URL_DEFAULT,
            constraint_description="Must be a valid http/https address or blank",
        )

        scope.solutions_template_options.add_parameter(
            self.email, "Email", ParameterSection.notification_configuration
        )
        scope.solutions_template_options.add_parameter(
            self.notebook_deploy,
            "Deploy Jupyter Notebook",
            ParameterSection.visualization_config,
        )
        scope.solutions_template_options.add_parameter(
            self.notebook_instance_type,
            "Jupyter Notebook Instance Type",
            ParameterSection.visualization_config,
        )
        scope.solutions_template_options.add_parameter(
            self.notebook_volume_size,
            "Jupyter Notebook Volume Size",
            ParameterSection.visualization_config,
        )
        scope.solutions_template_options.add_parameter(
            self.quicksight_analysis_owner,
            "(Optional) Deploy QuickSight Dashboard",
            ParameterSection.visualization_config,
        )
        scope.solutions_template_options.add_parameter(
            self.forecast_kms_key_arn,
            "(Optional) KMS key ARN used to encrypt Datasets and Predictors managed by Amazon Forecast",
            ParameterSection.security_config,
        )
        scope.solutions_template_options.add_parameter(
            self.forecast_deploy,
            "Demo / Forecast Deployment",
            ParameterSection.forecast_config,
        )
        scope.solutions_template_options.add_parameter(
            self.forecast_name,
            "(Optional) Forecast Name",
            ParameterSection.forecast_config,
        )
        scope.solutions_template_options.add_parameter(
            self.forecast_defaults_url,
            "(Optional) Default forecast configuration file URL",
            ParameterSection.forecast_config,
        )
        scope.solutions_template_options.add_parameter(
            self.tts_url,
            "(Optional) Target Time Series URL",
            ParameterSection.dataset_config,
        )
        scope.solutions_template_options.add_parameter(
            self.rts_url,
            "(Optional) Related Time Series URL",
            ParameterSection.dataset_config,
        )
        scope.solutions_template_options.add_parameter(
            self.md_url,
            "(Optional) Item Metadata URL",
            ParameterSection.dataset_config,
        )
        scope.solutions_template_options.add_parameter(
            self.lambda_log_level,
            "CloudWatch Log Level",
            ParameterSection.deployment_config,
        )
