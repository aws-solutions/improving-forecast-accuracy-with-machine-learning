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
from shared.status import Status


@step_function_step
def createdatasetimportjob(event, context) -> (Status, str):
    """
    Create/ monitor Amazon Forecast dataset import job creation
    :param event: lambda event
    :param context: lambda context
    :return: dataset import job status and dataset ARN
    """
    config = Config.from_sfn(event)
    dataset_file = DatasetFile(event.get("dataset_file"), event.get("bucket"))

    dataset_import = config.dataset_import_job(dataset_file)
    if dataset_import.status == Status.DOES_NOT_EXIST:
        dataset_import.create()

    return dataset_import.status, dataset_import.arn
