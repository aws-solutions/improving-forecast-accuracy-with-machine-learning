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

import json
import os
from typing import Dict, Any

from shared.Dataset.dataset_file import DatasetFile
from shared.helpers import get_sns_client
from shared.logging import get_logger

logger = get_logger(__name__)
UNKNOWN_SOURCE = "UNKNOWN"


def topic_arn():
    """
    Get the SNS topic ARN from environment variable
    :return: The SNS topic ARN
    """
    return os.environ["SNS_TOPIC_ARN"]


def solution_name() -> str:
    """
    Get the Solution Name from environment variable
    :return: the solution name
    """
    return os.environ["SOLUTION_NAME"]


class MessageBuilder:
    """Builds error messages from AWS Step Functions Output"""

    def __init__(self, event: Dict, context: Any):
        self.file = DatasetFile(event.get("dataset_file"), event.get("bucket"))
        self.dataset_group = event.get("dataset_group_name", self.file.prefix)

        self.error = event.get("error", {})
        self.states_error = self.error.get("statesError", None)
        self.service_error = self.error.get("serviceError", None)
        self.region = context.invoked_function_arn.split(":")[3]
        self.partition = context.invoked_function_arn.split(":")[1]
        self.account = context.invoked_function_arn.split(":")[4]

        if self.error:
            self.message = self._build_error_message()
        else:
            self.message = self._build_success_message()

        self.default = self._build_default_message()
        self.sms = self._build_sms_message()
        self.json = self._build_json_message()

    def _build_default_message(self) -> str:
        return f"Forecast for {self.dataset_group} completed {'with errors' if self.error else 'successfully'}"

    def _build_sms_message(self) -> str:
        return self._build_default_message()

    def _build_error_message(self) -> str:
        """
        Build the error message
        :return: the error message
        """
        if self.states_error:
            cause = json.loads(self.states_error.get("Cause", "{}"))
        elif self.service_error:
            cause = json.loads(self.service_error.get("Cause", "{}"))
        else:
            try:
                cause = json.loads(self.error.get("Cause"))
            except json.JSONDecodeError:
                cause = {"errorMessage": self.error.get("Cause")}

        error_detail = cause.get(
            "errorMessage", cause.get("ErrorMessage", UNKNOWN_SOURCE)
        )

        message = f"There was an error running the forecast job for dataset group {self.dataset_group}\n\n"
        message += f"Message: {error_detail}"
        return message

    def _build_success_message(self) -> str:
        """
        Build the success message
        :return: the success message
        """
        console_link = f"https://console.aws.amazon.com/forecast/home?region={self.region}#datasetGroups/arn:{self.partition}:forecast:{self.region}:{self.account}:dataset-group${self.dataset_group}/dashboard"

        message = (
            f"The forecast job for dataset group {self.dataset_group} is complete\n\n"
        )
        message += f"Link: {console_link}"
        return message

    def _build_json_message(self) -> str:
        return json.dumps(
            {
                "datasetGroup": self.dataset_group,
                "status": "UPDATE FAILED" if self.error else "UPDATE COMPLETE",
                "summary": self._build_default_message(),
                "description": self.message,
            }
        )


def sns(event, context):
    """
    Send an SNS message
    :param event: Lambda event
    :param context: Lambda context
    :return: None
    """
    sns = get_sns_client()
    message_builder = MessageBuilder(event, context)
    subject = f"{solution_name()} Notifications"

    logger.info(f"publishing message for event {event}")
    sns.publish(
        TopicArn=topic_arn(),
        Message=json.dumps(
            {
                "default": message_builder.default,
                "sms": message_builder.sms,
                "email": message_builder.message,
                "email-json": message_builder.json,
                "sqs": message_builder.json,
            }
        ),
        MessageStructure="json",
        Subject=subject,
    )
