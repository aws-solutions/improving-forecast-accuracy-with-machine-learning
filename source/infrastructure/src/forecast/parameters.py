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
from collections import UserList
from dataclasses import dataclass, field, fields
from typing import Union

from aws_cdk.core import Stack, NestedStack, CfnParameter

from interfaces import TemplateOptions

logger = logging.getLogger("cdk-helper")


@dataclass
class ParameterLabel:
    """A CfnParameter and its label for TemplateOptions"""

    parameter: CfnParameter = field(repr=False)
    label: str


class ParameterLabels(UserList):
    """Helper class to store and create parameter labels"""

    def create(self, options: TemplateOptions):
        for item in self.data:
            options.add_parameter_label(item.parameter, item.label)


@dataclass
class Parameters:
    """Create the parameters required for this stack"""

    parent: Union[Stack, NestedStack]
    limited_parameters: bool = field(repr=False, default=False)

    email: CfnParameter = field(repr=False, init=False)
    lambda_log_level: CfnParameter = field(repr=False, init=False)
    notebook_deploy: CfnParameter = field(repr=False, init=False)
    notebook_volume_size: CfnParameter = field(repr=False, init=False)
    notebook_instance_type: CfnParameter = field(repr=False, init=False)
    quicksight_analysis_owner: CfnParameter = field(repr=False, init=False)
    template_options: TemplateOptions = field(repr=False, init=False)

    def __post_init__(self):
        self.setup_parameters()

    def for_deployment(self, overrides: Union[dict, None] = None):
        """Provide all enabled parameters as a dictionary (useful for nested stacks)"""
        overrides = {} if not overrides else overrides
        parameters = {}
        for field in fields(self):
            if field.type == CfnParameter:
                try:
                    parameter = getattr(self, field.name)  # type: CfnParameter
                    value = overrides.get(parameter.node.id, parameter.value_as_string)
                    parameters[parameter.node.id] = value
                except AttributeError:
                    logger.info("ignoring limited parameter %s" % field.name)
        return parameters

    def setup_parameters(self):
        self.email = CfnParameter(
            self.parent,
            id="Email",
            type="String",
            description="Email to notify with forecast results",
            default="",
            max_length=50,
            allowed_pattern=r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$|^$)",
            constraint_description="Must be a valid email address or blank",
        )

        self.lambda_log_level = CfnParameter(
            self.parent,
            id="LambdaLogLevel",
            type="String",
            description="Change the verbosity of the logs output to CloudWatch",
            default="WARNING",
            allowed_values=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        )

        # hide these parameters:
        if not self.limited_parameters:
            self.notebook_deploy = CfnParameter(
                self.parent,
                id="NotebookDeploy",
                type="String",
                description="Deploy an Amazon SageMaker Jupyter Notebook instance",
                default="No",
                allowed_values=["Yes", "No"],
            )

            self.notebook_volume_size = CfnParameter(
                self.parent,
                id="NotebookVolumeSize",
                type="Number",
                description="Enter the size of the notebook instance EBS volume in GB",
                default=10,
                min_value=5,
                max_value=16384,
                constraint_description="Must be an integer between 5 (GB) and 16384 (16 TB)",
            )

            self.notebook_instance_type = CfnParameter(
                self.parent,
                id="NotebookInstanceType",
                type="String",
                description="Enter the type of the notebook instance",
                default="ml.t2.medium",
                allowed_values=[
                    "ml.t2.medium",
                    "ml.t3.medium",
                    "ml.r5.large",
                    "ml.c5.large",
                ],
            )

            self.quicksight_analysis_owner = CfnParameter(
                self.parent,
                id="QuickSightAnalysisOwner",
                description="With QuickSight Enterprise enabled, provide a QuickSight ADMIN user ARN to automatically create QuickSight analyses",
                default="",
                allowed_pattern="(^arn:.*:quicksight:.*:.*:user.*$|^$)",
            )

            self.forecast_kms_key_arn = CfnParameter(
                self.parent,
                id="ForecastKmsKeyArn",
                description="Provide Amazon Forecast with an alternate AWS Key Management (KMS) key to use for CreatePredictor and CreateDataset operations",
                default="",
                allowed_pattern="(^arn:.*:kms:.*:.*:key/.*$|^$)",
            )

    def add_template_options(self, options: TemplateOptions):
        labels = []

        options.add_parameter_group(
            label="Improving Forecast Accuracy with Machine Learning Configuration",
            parameters=[self.email],
        )
        labels.append(ParameterLabel(self.email, "Email"))

        if not self.limited_parameters:
            options.add_parameter_group(
                label="Visualization Options",
                parameters=[
                    self.quicksight_analysis_owner,
                    self.notebook_deploy,
                    self.notebook_instance_type,
                    self.notebook_volume_size,
                ],
            )
            labels.extend(
                [
                    ParameterLabel(self.notebook_deploy, "Deploy Jupyter Notebook"),
                    ParameterLabel(
                        self.notebook_volume_size, "Jupyter Notebook Volume Size"
                    ),
                    ParameterLabel(
                        self.notebook_instance_type, "Jupyter Notebook Instance Type"
                    ),
                    ParameterLabel(
                        self.quicksight_analysis_owner,
                        "(Optional) Deploy QuickSight Dashboard",
                    ),
                ]
            )
            options.add_parameter_group(
                label="Security Configuration", parameters=[self.forecast_kms_key_arn]
            )
            labels.append(
                ParameterLabel(
                    self.forecast_kms_key_arn,
                    "(Optional) KMS key ARN used to encrypt Datasets and Predictors managed by Amazon Forecast",
                )
            )

        options.add_parameter_group(
            label="Deployment Configuration", parameters=[self.lambda_log_level]
        )
        labels.append(ParameterLabel(self.lambda_log_level, "CloudWatch Log Level"))

        labels = ParameterLabels(labels)
        labels.create(options)
