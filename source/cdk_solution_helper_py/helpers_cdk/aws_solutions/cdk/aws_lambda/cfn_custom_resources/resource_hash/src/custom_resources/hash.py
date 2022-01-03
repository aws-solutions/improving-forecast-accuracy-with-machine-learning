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
from hashlib import md5
from os import getenv

from crhelper import CfnResource

logger = logging.getLogger(__name__)
helper = CfnResource(log_level=getenv("LOG_LEVEL", "WARNING"))


class StackId:
    def __init__(self, event):
        self.stack_id = event.get("StackId")
        self.partition = self.get_arn_component(1)
        self.service = self.get_arn_component(2)
        self.region = self.get_arn_component(3)
        self.account = self.get_arn_component(4)
        self.stack_name = self.get_arn_component(5).split("/")[1]

    def get_arn_component(self, idx: int) -> str:
        return self.stack_id.split(":")[idx]

    @property
    def hash(self):
        digest = md5()  # NOSONAR - safe to hash, not for cryptographic purposes
        digest.update(bytes(f"{self.stack_id.rsplit('/', 1)[0]}", "ascii"))
        return digest.hexdigest().upper()


def get_property(event, property_name, property_default=None):
    resource_prop = event.get("ResourceProperties", {}).get(
        property_name, property_default
    )
    if not resource_prop:
        raise ValueError(f"missing required property {property_name}")
    return resource_prop


@helper.create
def generate_hash(event, _):
    """
    Generate a resource name containing a hash of the stack ID (without unique ID) and resource purpose.
    This is useful when you need to create named IAM roles

    :param event: The CloudFormation custom resource event
    :return: None
    """
    stack_id = StackId(event)
    purpose = get_property(event, "Purpose")
    max_length = int(get_property(event, "MaxLength", 64))

    name = f"{purpose}-{stack_id.hash[:8]}"

    if len(name) > max_length:
        raise ValueError(
            f"the derived resource name {name} is too long ({len(name)} / {max_length}) - please use a shorter Purpose"
        )

    logger.info(f"the derived resource name is {name}")
    helper.Data["Name"] = name
    helper.Data["Id"] = stack_id.hash


@helper.update
@helper.delete
def no_op(_, __):
    pass  # pragma: no cover


def handler(event, _):
    """
    Handler entrypoint - see generate_hash for implementation details
    :param event: The CloudFormation custom resource event
    :return: PhysicalResourceId
    """
    helper(event, _)  # pragma: no cover
