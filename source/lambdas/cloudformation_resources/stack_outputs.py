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
from os import getenv

import boto3
from crhelper import CfnResource

logger = logging.getLogger(__name__)
helper = CfnResource(log_level=getenv("LOG_LEVEL", "WARNING"))


def get_property(event, property_name, property_default=None):
    property = event.get("ResourceProperties", {}).get(property_name, property_default)
    if not property:
        raise ValueError(f"missing required property {property_name}")
    return property


@helper.create
def stack_outputs(event, _):
    """
    Collect stack outputs from an existing stack, expose them as attributes to this custom resource

    :param event: The CloudFormation custom resource event
    :return: None
    """
    stack_name = get_property(event, "Stack")

    cli = boto3.client("cloudformation", region_name=getenv("AWS_REGION"))

    try:
        stack = cli.describe_stacks(StackName=stack_name)["Stacks"][0]
    except cli.exceptions.ClientError:
        raise ValueError("stack named %s does not exist" % stack_name)

    helper.Data.update(
        {item["OutputKey"]: item["OutputValue"] for item in stack.get("Outputs")}
    )


@helper.update
@helper.delete
def no_op(_, __):
    pass  # pragma: no cover


def handler(event, _):
    """
    Handler entrypoint - see stack_outputs for implementation details
    :param event: The CloudFormation custom resource event
    :return: PhysicalResourceId
    """
    helper(event, _)  # pragma: no cover
