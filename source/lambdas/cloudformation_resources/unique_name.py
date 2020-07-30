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

DEFAULT_LENGTH = 32
helper = CfnResource(log_level="INFO")


@helper.create
def generate_name(event, _):
    """
    Generate a physical resource ID limited in length to event['Length']
    :param event: The CloudFormation custom resource event
    :return: PhysicalResourceId
    """
    length = DEFAULT_LENGTH
    try:
        length = int(event.get("Length", DEFAULT_LENGTH))
    except ValueError:
        pass  # use DEFAULT_LENGTH if the length doesn't convert to an int

    unique_id = uuid().hex[:length]
    helper.Data["Id"] = unique_id
    return event.get("PhysicalResourceId", unique_id)


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
