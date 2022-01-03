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
from uuid import uuid4 as uuid

from crhelper import CfnResource

logger = logging.getLogger(__name__)
helper = CfnResource(log_level=getenv("LOG_LEVEL", "WARNING"))


def get_property(event, property_name, property_default=None):
    resource_prop = event.get("ResourceProperties", {}).get(
        property_name, property_default
    )
    if not resource_prop:
        raise ValueError(f"missing required property {property_name}")
    return resource_prop


@helper.create
def generate_name(event, _):
    """
    Generate a resource name containing the stack name and the resource purpose. This is useful
    when you need to associate policies that refer to a resource by name (and thus need
    a predictable resource name). This is commonly used when associating policies with buckets
    or other resources that might introduce a circular resource dependency

    :param event: The CloudFormation custom resource event
    :return: None
    """
    resource_id = get_property(event, "Id", uuid().hex[0:12])
    stack_name = get_property(event, "StackName")
    purpose = get_property(event, "Purpose")
    max_length = int(get_property(event, "MaxLength"))

    name = f"{stack_name}-{purpose}-{resource_id}".lower()
    if len(name) > max_length:
        logger.warning("cannot use stack name in bucket name - trying default")
        name = f"{purpose}-{resource_id}".lower()
    if len(name) > max_length:
        raise ValueError(
            f"the derived resource name {name} is too long ({len(name)} / {max_length}) - please use a shorter purpose or stack name"
        )

    logger.info(f"the derived resource name is {name}")
    helper.Data["Name"] = name
    helper.Data["Id"] = resource_id


@helper.update
@helper.delete
def no_op(_, __):
    pass  # pragma: no cover


def handler(event, _):
    """
    Handler entrypoint - see generate_name for implementation details
    :param event: The CloudFormation custom resource event
    :return: PhysicalResourceId
    """
    helper(event, _)  # pragma: no cover
