#!/usr/bin/env python3

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

import forecast.sagemaker.notebook
import pytest
import quicksight
import aws_cdk as cdk
from aws_cdk.assertions import Capture, Template, Match
from aws_solutions.cdk import CDKSolution
from forecast.stack import ForecastStack

solution = CDKSolution(cdk_json_path=Path(__file__).parent.absolute() / "cdk.json")

from aspects.app_registry import AppRegistry


@pytest.fixture(scope="module")
def synth_template():
    app = cdk.App(
        context={
            "SOLUTION_NAME": "Improving Forecast Accuracy with Machine Learning",
            "SOLUTION_ID": "SO0123",
            "SOLUTION_VERSION": "v1.4.0",
            "APP_REG_NAME": "improving_forecast_accuracy_with_machine_learning",
            "APPLICATION_TYPE": "AWS-Solutions",
            "VERSION": "1.4.0",
            "BUCKET_NAME": "test_bucket",
            "NOTEBOOKS": forecast.sagemaker.notebook.context,
        }
    )

    stack = ForecastStack(
        app,
        "forecast-stack-cdk",
        description=f"Automate Amazon Forecast predictor and forecast generation and visualize forecasts via Amazon QuickSight or an Amazon SageMaker Jupyter Notebook",
        template_filename="improving-forecast-accuracy-with-machine-learning.template",
        synthesizer=solution.synthesizer,
        extra_mappings=quicksight.TemplateSource(
            solution_name=app.node.try_get_context("SOLUTION_NAME"),
            solution_version=app.node.try_get_context("SOLUTION_VERSION"),
        ).mappings,
    )
    cdk.Aspects.of(app).add(AppRegistry(stack, "AppRegistryAspect"))
    template = Template.from_stack(stack)
    yield template, app

app_registry_capture = Capture()

def test_service_catalog_registry_application(synth_template):
    template, app = synth_template
    template.resource_count_is("AWS::ServiceCatalogAppRegistry::Application", 1)
    template.has_resource_properties(
        "AWS::ServiceCatalogAppRegistry::Application",
        {
            "Name": {
                "Fn::Join": [
                    "-",
                    [
                        "App",
                        {"Ref": "AWS::StackName"},
                        app.node.try_get_context("APP_REG_NAME"),
                        {"Ref": "AWS::AccountId"},
                        {"Ref": "AWS::Region"},
                    ],
                ]
            },
            "Description": "Service Catalog application to track and manage all your resources for the solution Improving Forecast Accuracy with Machine Learning",
            "Tags": {
                "SOLUTION_ID": "SO0123",
                "SOLUTION_NAME": "Improving Forecast Accuracy with Machine Learning",
                "SOLUTION_VERSION": "v1.4.0",
                "Solutions:ApplicationType": "AWS-Solutions",
                "Solutions:SolutionID": "SO0123",
                "Solutions:SolutionName": "Improving Forecast Accuracy with Machine Learning",
                "Solutions:SolutionVersion": "v1.4.0",
            },
        },
    )

def test_resource_association_nested_stacks(synth_template):
    template, app = synth_template
    template.resource_count_is("AWS::ServiceCatalogAppRegistry::ResourceAssociation", 1)
    template.has_resource_properties(
        "AWS::ServiceCatalogAppRegistry::ResourceAssociation",
        {
            "Application": {"Fn::GetAtt": [app_registry_capture, "Id"]},
            "Resource": {"Ref": "AWS::StackId"},
            "ResourceType": "CFN_STACK",
        },
    )