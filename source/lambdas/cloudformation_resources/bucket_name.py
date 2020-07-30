# #####################################################################################################################
#  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                            #
#                                                                                                                     #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. A copy of the License is located at                                                              #
#                                                                                                                     #
#  http://www.apache.org/licenses/LICENSE-2.0                                                                         #
#                                                                                                                     #
#  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES  #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions     #
#  and limitations under the License.                                                                                 #
# #####################################################################################################################

from uuid import uuid4 as uuid

from crhelper import CfnResource


helper = CfnResource(log_level="INFO")


def get_property(event, property_name, property_default=None):
    property = event.get("ResourceProperties", {}).get(property_name, property_default)
    if not property:
        raise ValueError(f"missing required property {property_name}")
    return property


@helper.create
def generate_name(event, _):
    """
    Generate a bucket name containing the stack name and the bucket purpose. This is useful
    when you need to associate bucket policies that refer to a bucket by name (and thus need
    a predictable bucket name). This is commonly used when associating policies with buckets
    that have associated S3 notifications

    :param event: The CloudFormation custom resource event
    :return: None
    """
    id = get_property(event, "Id", uuid().hex)
    stack_name = get_property(event, "StackName")
    bucket_purpose = get_property(event, "BucketPurpose")

    bucket_name = f"{stack_name}-{bucket_purpose}-{id}".lower()
    length = len(bucket_name)
    if length > 63:
        raise ValueError(
            f"the derived bucket name {bucket_name} is too long - please use a shorter bucket purpose or stack name"
        )

    helper.Data["Name"] = bucket_name


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
