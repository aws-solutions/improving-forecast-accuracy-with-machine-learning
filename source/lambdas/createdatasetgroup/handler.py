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
from shared.status import Status


@step_function_step
def createdatasetgroup(event, context) -> (Status, str):
    """
    Create/ monitor Amazon Forecast dataset group creation
    :param event: lambda event
    :param context: lambda context
    :return: dataset group status and dataset group ARN
    """
    config = Config.from_sfn(event)
    dataset_file = DatasetFile(event.get("dataset_file"), event.get("bucket"))
    dataset_groups = config.dataset_groups(dataset_file)
    datasets = config.datasets(dataset_file)

    # dataset group creation returns an ARN immediately; creation of all dependent dataset groups is safe
    # dataset group update returns immediately; update of all dependent dataset groups is safe
    for dataset_group in dataset_groups:
        if dataset_group.status == Status.DOES_NOT_EXIST:
            dataset_group.create()

        if dataset_group.status != Status.ACTIVE:
            raise ValueError(
                f"Dataset group {dataset_group.dataset_group_name} is {dataset_group.status}, expected ACTIVE"
            )

        dataset_group.update(datasets, dataset_file)

    # at this point, we are guaranteed that all dataset groups are active (or an error was thrown)
    return (
        Status.ACTIVE,
        [dataset_group.dataset_group_name for dataset_group in dataset_groups],
    )
