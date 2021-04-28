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

from os import environ

import boto3
import pytest
from moto import mock_cloudformation

from lambdas.cloudformation_resources.stack_outputs import stack_outputs, helper

TEMPLATE = """
---
AWSTemplateFormatVersion: 2010-09-09

Resources: 
    CDKMetadata:
        Type: "AWS::CDK::Metadata"
        Properties: 
            Modules: some-list

Outputs: 
    OutputOne: 
        Description: This is output number one
        Value: output_1
    OutputTwo: 
        Description: This is output number two 
        Value: output_2
"""


@pytest.fixture
def stack_name():
    return "StackName"


@pytest.fixture
def lambda_event(stack_name):
    event = {"ResourceProperties": {"Stack": stack_name}}
    yield event


@mock_cloudformation
def test_get_stack_output_not_exists(lambda_event):
    with pytest.raises(ValueError):
        stack_outputs(lambda_event, None)


@mock_cloudformation
def test_get_stack_output_exists(lambda_event, stack_name):
    cli = boto3.client("cloudformation", region_name=environ["AWS_REGION"])
    cli.create_stack(
        StackName=stack_name,
        TemplateBody=TEMPLATE,
    )
    stack_outputs(lambda_event, None)

    assert helper.Data["OutputOne"] == "output_1"
    assert helper.Data["OutputTwo"] == "output_2"
