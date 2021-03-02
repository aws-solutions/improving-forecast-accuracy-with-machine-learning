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
from __future__ import annotations

from dataclasses import dataclass, field
from operator import itemgetter
from typing import List

from shared.Dataset.dataset import Dataset
from shared.Dataset.dataset_domain import DatasetDomain
from shared.Dataset.dataset_file import DatasetFile
from shared.DatasetGroup.dataset_group_name import DatasetGroupName
from shared.DatasetGroup.schemas import SCHEMAS_DEF
from shared.helpers import ForecastClient, DatasetsImporting
from shared.logging import get_logger
from shared.status import Status

logger = get_logger(__name__)


LATEST_DATASET_UPDATE_FILENAME_TAG = "LatestDatasetUpdateName"
LATEST_DATASET_UPDATE_FILE_ETAG_TAG = "LatestDatasetUpdateETag"


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
            logger.debug(
                "Dataset Group %s not found - will attempt to create"
                % self._dataset_group_name
            )

        try:
            self.cli.create_dataset_group(
                DatasetGroupName=str(self._dataset_group_name),
                Domain=str(self._dataset_group_domain),
                Tags=self.tags,
            )
        except self.cli.exceptions.ResourceAlreadyExistsException:
            logger.debug("Dataset Group %s already exists" % self._dataset_group_name)

    def update(self, datasets: List[Dataset], dataset_file: DatasetFile):
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
        self.cli.tag_resource(
            ResourceArn=self.arn,
            Tags=[
                {
                    "Key": LATEST_DATASET_UPDATE_FILENAME_TAG,
                    "Value": dataset_file.filename,
                },
                {
                    "Key": LATEST_DATASET_UPDATE_FILE_ETAG_TAG,
                    "Value": dataset_file.etag,
                },
            ],
        )

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

    def ready(self) -> bool:
        """
        Ensure this dataset group is ready (all defined datasets are ACTIVE). Raise an exception if not
        :return: bool
        """
        datasets = self.datasets

        # ensure each dataset is active
        datasets_ready = all(dataset.get("Status") == "ACTIVE" for dataset in datasets)
        if not datasets_ready:
            msg = f"One or more of the datasets for dataset group {self._dataset_group_name} is not yet ACTIVE\n\n"
            for dataset in datasets:
                msg += f"Dataset {dataset.get('DatasetName')} had status {dataset.get('Status')}\n"
            raise DatasetsImporting(msg)

        # check there is an active import for each dataset
        msg = ""
        for dataset in datasets:
            imports = []
            paginator = self.cli.get_paginator("list_dataset_import_jobs")
            iterator = paginator.paginate(
                Filters=[
                    {
                        "Condition": "IS",
                        "Key": "DatasetArn",
                        "Value": dataset["DatasetArn"],
                    },
                    {"Condition": "IS", "Key": "Status", "Value": "ACTIVE"},
                ]
            )
            for page in iterator:
                imports.extend(page.get("DatasetImportJobs", []))
            imports = sorted(
                imports, key=itemgetter("LastModificationTime"), reverse=True
            )
            if len(imports) == 0:
                msg += f"no ACTIVE datasets for {dataset.get('DatasetArn')}\n"
            else:
                status = imports[0].get("Status")
                if status != "ACTIVE":
                    msg += f"no ACTIVE dataset for {dataset.get('DatasetArn')} - status was {status}\n"
        if msg:
            raise DatasetsImporting(msg)

        return True

    @property
    def latest_timestamp(self, format="%Y_%m_%d_%H_%M_%S"):
        latest_dataset_modified = max(
            [dataset.get("LastModificationTime") for dataset in self.datasets]
        )
        if format:
            return latest_dataset_modified.strftime(format)
        else:
            return latest_dataset_modified

    @property
    def schema(self) -> Schema:
        return Schema(self)


@dataclass
class Schema:
    dataset_group: DatasetGroup
    fields: List[str] = field(init=False)
    identifier: str = field(init=False)
    metric: str = field(init=False)
    date: str = field(init=False)

    def __post_init__(self):
        properties = SCHEMAS_DEF.get(str(self.dataset_group.dataset_group_domain))
        self.fields = properties.get("fields")
        self.identifier = properties.get("identifier")
        self.metric = properties.get("metric")
        self.date = "timestamp"
