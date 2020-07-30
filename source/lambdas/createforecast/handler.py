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

from shared.Dataset.dataset_file import DatasetFile
from shared.config import Config
from shared.helpers import step_function_step
from shared.logging import get_logger
from shared.status import Status

logger = get_logger(__name__)


@step_function_step
def createforecast(event, context):
    """
    Create/ monitor Amazon Forecast forecast creation
    :param event: lambda event
    :param context: lambda context
    :return: forecast / forecast export status and forecast ARN
    """
    config = Config.from_sfn(event)
    dataset_file = DatasetFile(event.get("dataset_file"), event.get("bucket"))

    forecast = config.forecast(dataset_file)
    tracked = forecast

    if forecast.status == Status.DOES_NOT_EXIST:
        # TODO: publish predictor stats to CloudWatch prior to create
        logger.info("Creating forecast for %s" % dataset_file.prefix)
        forecast.create()

    if forecast.status == Status.ACTIVE:
        logger.info("Creating forecast export for %s" % dataset_file.prefix)
        tracked = forecast.export(dataset_file)

    return tracked.status, forecast.arn
