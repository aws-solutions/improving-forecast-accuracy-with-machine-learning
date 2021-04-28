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

from shared.Dataset.dataset_file import DatasetFile
from shared.config import Config
from shared.helpers import step_function_step
from shared.logging import get_logger
from shared.status import Status

logger = get_logger(__name__)


@step_function_step
def handler(event, context):
    """
    Create/ monitor Amazon Forecast forecast creation
    :param event: lambda event
    :param context: lambda context
    :return: forecast / forecast export status and forecast ARN
    """
    config = Config.from_sfn(event)
    dataset_file = DatasetFile(event.get("dataset_file"), event.get("bucket"))
    dataset_group_name = event.get("dataset_group_name")

    forecast = config.forecast(dataset_file, dataset_group_name)

    if forecast.status == Status.DOES_NOT_EXIST:
        # TODO: publish predictor stats to CloudWatch prior to create
        logger.info("Creating forecast for %s" % dataset_group_name)
        forecast.create()

    return forecast.status, forecast.arn
