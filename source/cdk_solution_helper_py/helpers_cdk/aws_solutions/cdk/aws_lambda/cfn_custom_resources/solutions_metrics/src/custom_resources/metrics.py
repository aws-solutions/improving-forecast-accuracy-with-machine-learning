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
import uuid
from datetime import datetime
from os import getenv

import requests
from crhelper import CfnResource

logger = logging.getLogger(__name__)
helper = CfnResource(log_level=getenv("LOG_LEVEL", "WARNING"))
METRICS_ENDPOINT = "https://metrics.awssolutionsbuilder.com/generic"


def _sanitize_data(event):
    resource_properties = event["ResourceProperties"]
    # Remove ServiceToken (lambda arn) to avoid sending AccountId
    resource_properties.pop("ServiceToken", None)
    resource_properties.pop("Resource", None)

    # Solution ID and unique ID are sent separately
    resource_properties.pop("Solution", None)
    resource_properties.pop("UUID", None)

    # Add some useful fields related to stack change
    resource_properties["CFTemplate"] = (
        event["RequestType"] + "d"
    )  # Created, Updated, or Deleted

    return resource_properties


@helper.create
@helper.update
@helper.delete
def send_metrics(event, _):
    resource_properties = event["ResourceProperties"]
    random_id = event.get("PhysicalResourceId", str(uuid.uuid4()))
    helper.Data["UUID"] = random_id

    try:
        headers = {"Content-Type": "application/json"}
        payload = {
            "Solution": resource_properties["Solution"],
            "UUID": random_id,
            "TimeStamp": datetime.utcnow().isoformat(),
            "Data": _sanitize_data(event),
        }

        logger.info(f"Sending payload: {payload}")
        response = requests.post(METRICS_ENDPOINT, json=payload, headers=headers)
        logger.info(
            f"Response from metrics endpoint: {response.status_code} {response.reason}"
        )
        if "stackTrace" in response.text:
            logger.exception("Error submitting usage data: %s" % response.text)
        # raise when there is an HTTP error (non success code)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        logger.exception(f"Could not send usage data: {exc}")
    except Exception as exc:
        logger.exception(f"Unknown error when trying to send usage data: {exc}")

    return random_id


def handler(event, context):
    helper(event, context)  # pragma: no cover
