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
from os import environ

from shared.config import Config, ConfigNotFound
from shared.helpers import get_sfn_client
from shared.logging import get_logger
from shared.s3.notification import Event

logger = get_logger(__name__)


def notification(event: dict, context):
    """Handles an S3 Event Notification (for any .csv file written to any key under train/*)

    :param dict event: AWS Lambda Event (in this case, an S3 Event message)
    :param context: The AWS Lambda Context object
    :return: None
    """

    # Get the event data, then read the default config file
    evt = Event(event)
    s3_config = None

    # Build the input to the state machine
    state_input = {"bucket": evt.bucket, "dataset_file": evt.key}
    logger.info(
        "Triggered by s3 notification on bucket %s, key %s" % (evt.bucket, evt.key)
    )
    try:
        s3_config = Config.from_s3(evt.bucket)
        state_input["config"] = s3_config.config
    except ConfigNotFound as excinfo:
        logger.warning("The configuration file was not found")
        state_input["error"] = {
            "serviceError": {
                "Error": "ConfigNotFound",
                "Cause": json.dumps({"errorMessage": str(excinfo)}),
            }
        }
    except ValueError as excinfo:
        logger.warning("There was a problem with the config file: %s" % str(excinfo))
        state_input["error"] = {
            "serviceError": {
                "Error": "ValueError",
                "Cause": json.dumps({"errorMessage": str(excinfo)}),
            }
        }

    # validate the config file if it loaded properly
    if s3_config:
        errors = s3_config.validate()
        if errors:
            for error in errors:
                logger.warning("config problem: %s" % error)

            state_input["error"] = {
                "serviceError": {
                    "Error": "ConfigError",
                    "Cause": json.dumps({"errorMessage": "\n".join(errors)}),
                }
            }

    # Start the AWS Step Function automation of Amazon Forecast
    sfn = get_sfn_client()
    sfn.start_execution(
        stateMachineArn=environ.get("STATE_MACHINE_ARN"),
        name=evt.event_id,
        input=json.dumps(state_input),
    )
