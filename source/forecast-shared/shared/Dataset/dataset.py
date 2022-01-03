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

from operator import itemgetter
from os import environ
from typing import Union, Optional

from botocore.exceptions import ClientError

from shared.Dataset.data_frequency import DataFrequency
from shared.Dataset.dataset_domain import DatasetDomain
from shared.Dataset.dataset_name import DatasetName
from shared.Dataset.dataset_type import DatasetType
from shared.helpers import ForecastClient, UserTags
from shared.logging import get_logger
from shared.status import Status

logger = get_logger(__name__)


class Dataset(ForecastClient):
    """Represents the desired state of a dataset stored in Amazon Forecast"""

    def __init__(
        self,
        dataset_name: DatasetName,
        dataset_type: DatasetType,
        dataset_domain: DatasetDomain,
        dataset_schema: dict,
        data_frequency: DataFrequency = None,
        user_tags: Optional[UserTags] = None,
    ):
        self._dataset_name = dataset_name
        self._dataset_type = dataset_type
        self._dataset_domain = dataset_domain
        self._data_frequency = data_frequency
        self._dataset_schema = dataset_schema

        self._params = {
            "DatasetName": str(self._dataset_name),
            "DatasetType": str(self._dataset_type),
            "Domain": str(self._dataset_domain),
            "Schema": self._dataset_schema,
        }
        if self._data_frequency:
            self._params["DataFrequency"] = str(self._data_frequency.frequency)

        super().__init__(resource="dataset", user_tags=user_tags, **self._params)

    def __repr__(self):
        return f'Dataset(dataset_name="{self._dataset_name}", dataset_type="{self._dataset_type}", dataset_domain="{self._dataset_domain}")'

    @property
    def arn(self) -> str:
        """
        Get the ARN of this resource
        :return: The ARN of this resource
        """
        dataset_arn = f"arn:aws:forecast:{self.region}:{self.account_id}:dataset/{self.dataset_name}"
        return dataset_arn

    @property
    def dataset_name(self) -> DatasetName:
        """
        Get the dataset name of this resource
        :return: The dataset name of this resource
        """
        return self._dataset_name

    @property
    def dataset_type(self) -> DatasetType:
        """
        Get the dataset type of this resource
        :return: The dataset type of this resource
        """
        return self._dataset_type

    @property
    def dataset_domain(self) -> DatasetDomain:
        """
        Get the dataset domain of this resource
        :return: The dataset domain of this resource
        """
        return self._dataset_domain

    @property
    def data_frequency(self) -> Optional[DataFrequency]:
        """
        Get the data frequency of this resource (applicable to timeseries, and related timeseries only)
        :return: The data frequency of this resource if it exists
        """
        if self._dataset_type == DatasetType.ITEM_METADATA:
            return None
        return self._data_frequency

    @property
    def dataset_schema(self) -> dict:
        """
        Get the schema of this resource
        :return: The schema of this resource
        """
        return self._dataset_schema

    @property
    def status(self) -> Status:
        """
        Get the status of this dataset as defined.
        :return:
        """
        dataset_status = Status.DOES_NOT_EXIST
        try:
            dataset_info = self.cli.describe_dataset(DatasetArn=self.arn)
            dataset_status = Status[dataset_info.get("Status")]
        except self.cli.exceptions.ResourceNotFoundException:
            pass  # status should be DOES_NOT_EXIST as above

        return dataset_status

    @property
    def timestamp_format(self) -> Union[str, None]:
        """
        Get the most recent import job's timestamp format
        :return:
        """
        imports = self.imports
        if not imports:
            return None
        latest_import = imports[0]

        latest_import_details = self.cli.describe_dataset_import_job(
            DatasetImportJobArn=latest_import.get("DatasetImportJobArn")
        )
        return latest_import_details.get("TimestampFormat")

    @property
    def imports(self):
        """
        Get the history of dataset imports for this dataset from the Amazon Forecast service.
        :return: List of ACTIVE dataset imports, in descending order by creation time
        """
        paginator = self.cli.get_paginator("list_dataset_import_jobs")
        iterator = paginator.paginate(
            Filters=[
                {"Condition": "IS", "Key": "DatasetArn", "Value": self.arn},
                {"Condition": "IS", "Key": "Status", "Value": "ACTIVE"},
            ]
        )

        jobs = []
        for page in iterator:
            jobs.extend(page.get("DatasetImportJobs"))
        jobs = sorted(jobs, key=itemgetter("LastModificationTime"), reverse=True)

        return jobs

    def _create_params(self):
        """
        Append tags and EncryptionConfig to the parameters to pass to CreateDataset
        :return: the creation parameters
        """
        forecast_role = environ.get("FORECAST_ROLE", None)
        forecast_kms = environ.get("FORECAST_KMS", None)
        if forecast_role and forecast_kms:
            self._params["EncryptionConfig"] = {
                "KMSKeyArn": forecast_kms,
                "RoleArn": forecast_role,
            }
        self._params["Tags"] = self.tags
        return self._params

    def create(self):
        """
        Create the dataset
        :return: None
        """
        exceptions = []

        try:
            dataset_info = self.cli.describe_dataset(DatasetArn=self.arn)

            if dataset_info.get("DatasetType") != self._dataset_type:
                exceptions.append(
                    f"dataset type ({dataset_info.get('DatasetType')}) does not match expected ({self._dataset_type})"
                )
            if dataset_info.get("Domain") != self._dataset_domain:
                exceptions.append(
                    f"dataset domain ({dataset_info.get('Domain')}) does not match ({self.dataset_domain})"
                )
            if dataset_info.get("DataFrequency") != self._data_frequency:
                exceptions.append(
                    f"data frequency ({dataset_info.get('DataFrequency')}) does not match ({self._data_frequency})"
                )
            if dataset_info.get("Schema") != self._dataset_schema:
                exceptions.append("dataset schema does not match")

            if exceptions:
                raise ValueError("\n".join(exceptions))
        except ClientError as ex:
            if ex.response["Error"]["Code"] != "ResourceNotFoundException":
                raise ex

        try:
            self.cli.create_dataset(**self._create_params())
        except self.cli.exceptions.ResourceAlreadyExistsException:
            logger.debug("Dataset %s is already creating" % str(self._dataset_name))

        self.set_user_tags(
            resource_arn=self.arn,
        )
