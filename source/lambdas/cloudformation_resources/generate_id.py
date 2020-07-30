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


@helper.create
@helper.update
@helper.delete
def generate_id(event, _):
    """
    This resource will always generate a 12 hex digit random physical ID if its parameters have changed

    :param event: The CloudFormation custom resource event
    :return: the new 12 digit physical ID (also available in Data['Id'])
    """
    physical_id = uuid().hex[0:12]
    helper.Data["Id"] = physical_id
    return physical_id


def handler(event, _):
    """
    Handler entrypoint - see generate_id for implementation details
    :param event: The CloudFormation custom resource event
    :return: PhysicalResourceId
    """
    helper(event, _)  # pragma: no cover
