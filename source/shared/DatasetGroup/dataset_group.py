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

from typing import List

from shared.Dataset.dataset import Dataset
from shared.Dataset.dataset_domain import DatasetDomain
from shared.DatasetGroup.dataset_group_name import DatasetGroupName
from shared.helpers import ForecastClient
from shared.status import Status


class DatasetGroup(ForecastClient):
    """Used to hold the configuration of an Amazon Forecast dataset group"""

    def __init__(
        self, dataset_group_name: DatasetGroupName, dataset_domain: DatasetDomain
    ):
        self._dataset_group_name = dataset_group_name
        self._dataset_group_domain = dataset_domain

        super().__init__(
            resource="dataset_group",
            DatasetGroupName=str(self._dataset_group_name),
            Domain=str(self._dataset_group_domain),
        )

    @property
    def arn(self) -> str:
        """
        Get the ARN of this resource
        :return: The ARN of this resrource if it exists, otherwise None
        """
        dataset_arn = f"arn:aws:forecast:{self.region}:{self.account_id}:dataset-group/{self.dataset_group_name}"
        return dataset_arn

    @property
    def dataset_group_name(self) -> DatasetGroupName:
        """
        Get the dataset group name for this resource
        :return: The dataset group name
        """
        return self._dataset_group_name

    @property
    def dataset_group_domain(self) -> DatasetDomain:
        """
        Get the dataset group domain for this resource
        :return: The dataset group domain
        """
        return self._dataset_group_domain

    @property
    def status(self) -> Status:
        """
        Get the status of this dataset group.
        :return: Status
        """
        dataset_group_status = Status.DOES_NOT_EXIST

        try:
            dataset_group_info = self.cli.describe_dataset_group(
                DatasetGroupArn=self.arn
            )
            dataset_group_status = Status[dataset_group_info.get("Status")]
        except self.cli.exceptions.ResourceNotFoundException:
            pass

        return dataset_group_status

    def create(self):
        """
        Create this dataset group
        :return: None
        """
        try:
            dataset_group_info = self.cli.describe_dataset_group(
                DatasetGroupArn=self.arn
            )

            service_domain = dataset_group_info.get("Domain")
            if service_domain != self._dataset_group_domain:
                raise ValueError(
                    f"dataset group domain ({service_domain}) does not match expected ({self._dataset_group_domain})"
                )

        except self.cli.exceptions.ResourceNotFoundException:
            pass

        self.cli.create_dataset_group(
            DatasetGroupName=str(self._dataset_group_name),
            Domain=str(self._dataset_group_domain),
        )

    def update(self, datasets: List[Dataset]):
        """
        Update this dataset group's assigned datasets
        :param datasets: The datasets to assign to this dataset group
        :return:
        """
        arns = [dataset.arn for dataset in datasets]

        # this is safe, the create dataset operation isn't async
        for dataset in datasets:
            dataset.create()

        self.cli.update_dataset_group(DatasetGroupArn=self.arn, DatasetArns=arns)

    @property
    def datasets(self) -> List:
        """
        Get the datasets currently associated with this dataset group. The dataset group must exist or this will raise
        an exception
        :return: Dataset details for all datasets assigned to this Dataset Group
        """
        info = self.cli.describe_dataset_group(DatasetGroupArn=self.arn)
        dataset_arns = info.get("DatasetArns")

        datasets_info = [
            self.cli.describe_dataset(DatasetArn=dataset_arn)
            for dataset_arn in dataset_arns
        ]

        return datasets_info
