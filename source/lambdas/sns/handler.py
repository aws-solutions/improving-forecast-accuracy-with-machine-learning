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

import json
import os

from shared.Dataset.dataset_file import DatasetFile
from shared.helpers import get_sns_client
from shared.logging import get_logger

logger = get_logger(__name__)


def topic_arn():
    """
    Get the SNS topic ARN from environment variable
    :return: The SNS topic ARN
    """
    return os.environ["SNS_TOPIC_ARN"]


def build_message(event):
    """
    Build a message for SNS to publish
    :param event: the lambda event containing the message
    :return: the message to publish
    """
    message = ""
    error = None
    file = DatasetFile(event.get("dataset_file"), event.get("bucket"))

    if "statesError" in event.keys():
        logger.info("State error message encountered")
        message += f"There was an error running the forecast for {file.prefix}\n\n"
        error = event.get("statesError")
    if "serviceError" in event.keys():
        logger.info("Service error message encountered")
        message += (
            f"There was a service error running the forecast for {file.prefix}\n\n"
        )
        error = event.get("serviceError")

    if error:
        error_type = error.get("Error", "Unknown")
        error_cause = json.loads(error.get("Cause", "{}"))
        error_message = error_cause.get("errorMessage")
        stack_trace = error_cause.get("stackTrace")

        message += f"Message: {error_message}\n\n"
        message += f"Details: (caught {error_type})\n\n"
        if stack_trace:
            message += f"\n".join(stack_trace)
    else:
        message = f"Forecast for {file.prefix} is ready!"

    return message


def sns(event, context):
    """
    Send an SNS message
    :param event: Lambda event
    :param context: Lambda context
    :return: None
    """
    cli = get_sns_client()

    logger.info("Publishing message for event: %s" % event)
    cli.publish(TopicArn=topic_arn(), Message=build_message(event))


def sns_conditional(event, context):
    """Send an SNS message if 'serviceError' is specified in the input.
    :param event: Lambda event
    :param context: Lambda context
    :return: True if a message was sent, False otherwise
    """

    cli = get_sns_client()

    error = event.get("serviceError")
    if error:
        logger.info("Publishing message for event %s" % event)
        cli.publish(TopicArn=topic_arn(), Message=build_message(event))
        return True

    logger.info("No message to publish for event %s" % event)
    return False
