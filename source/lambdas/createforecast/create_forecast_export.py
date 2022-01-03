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
def handler(event, context) -> (Status, str):
    config = Config.from_sfn(event)
    dataset_file = DatasetFile(event.get("dataset_file"), event.get("bucket"))
    dataset_group_name = event.get("dataset_group_name")
    predictor_arn = event.get("PredictorArn")

    forecast = config.forecast(dataset_file, dataset_group_name, predictor_arn)

    if forecast.status == Status.ACTIVE:
        logger.info("Creating forecast export for %s" % dataset_file.prefix)
        export = forecast.export(dataset_file)
    else:
        raise ValueError("forecast status must be ACTIVE to export a forecast")

    return export.status, forecast.arn
