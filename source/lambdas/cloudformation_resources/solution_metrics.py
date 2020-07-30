######################################################################################################################
#  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                           #
#                                                                                                                    #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance    #
#  with the License. A copy of the License is located at                                                             #
#                                                                                                                    #
#      http://www.apache.org/licenses/LICENSE-2.0                                                                    #
#                                                                                                                    #
#  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions    #
#  and limitations under the License.                                                                                #
######################################################################################################################

import logging, uuid, requests
from crhelper import CfnResource
from datetime import datetime

logger = logging.getLogger(__name__)
helper = CfnResource(log_level='INFO')
METRICS_ENDPOINT = 'https://metrics.awssolutionsbuilder.com/generic'

def _sanitize_data(resource_properties):
    # Remove ServiceToken (lambda arn) to avoid sending AccountId
    resource_properties.pop('ServiceToken', None)
    resource_properties.pop('Resource', None)

    # Solution ID and unique ID are sent separately
    resource_properties.pop('Solution', None)
    resource_properties.pop('UUID', None)

    return resource_properties

@helper.create
def send_metrics(event, _):
    resource_properties = event['ResourceProperties']
    random_id = str(uuid.uuid4())
    helper.Data['UUID'] = random_id

    try:
        headers = { 'Content-Type': 'application/json' }
        payload = {
            'Solution': resource_properties['Solution'],
            'UUID': random_id,
            'TimeStamp': datetime.utcnow().isoformat(),
            'Data': _sanitize_data(resource_properties)
        }

        logger.info(f'Sending payload: {payload}')
        response = requests.post(METRICS_ENDPOINT, json=payload, headers=headers)
        logger.info(f'Response from metrics endpoint: {response.status_code} {response.reason}')
        if 'stackTrace' in response.text:
            logger.exception('Error submitting usage data: %s' % response.text)
    except requests.exceptions.RequestException:
        logger.exception('Could not send usage data')
    except Exception:
        logger.exception('Unknown error when trying to send usage data')

    return random_id

@helper.update
@helper.delete
def no_op(_, __):
    pass  # pragma: no cover

def handler(event, context):
    helper(event, context)  # pragma: no cover