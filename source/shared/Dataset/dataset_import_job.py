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

from operator import itemgetter
from os import environ

from shared.Dataset.data_timestamp_format import DataTimestampFormat
from shared.Dataset.dataset_file import DatasetFile
from shared.helpers import ForecastClient
from shared.logging import get_logger
from shared.status import Status

logger = get_logger(__name__)


class DatasetImportJob(ForecastClient):
    """Represents the desired state of a dataset import job generated by Amazon Forecast"""

    def __init__(
        self,
        dataset_file: DatasetFile,
        dataset_arn: str,
        timestamp_format: DataTimestampFormat,
        geolocation_format: str,
        time_zone: str,
        use_geolocation_for_time_zone: bool,
    ):
        self.dataset_arn = dataset_arn
        self.timestamp_format = timestamp_format
        self.dataset_file = dataset_file
        self.geolocation_format = geolocation_format
        self.use_geolocation_for_time_zone = use_geolocation_for_time_zone
        self.time_zone = time_zone
        self._import_job_params = {
            "DatasetImportJobName": "PLACEHOLDER",
            "DatasetArn": self.dataset_arn,
            "DataSource": {
                "S3Config": {
                    "Path": f"s3://{self.dataset_file.bucket}/{self.dataset_file.key}",
                    "RoleArn": environ.get("FORECAST_ROLE"),
                }
            },
        }
        if self.timestamp_format:
            self._import_job_params["TimestampFormat"] = str(self.timestamp_format)
        if self.geolocation_format:
            self._import_job_params["GeolocationFormat"] = self.geolocation_format
        if self.use_geolocation_for_time_zone:
            self._import_job_params[
                "UseGeolocationForTimeZone"
            ] = self.use_geolocation_for_time_zone
        if self.time_zone:
            self._import_job_params["TimeZone"] = self.time_zone

        super().__init__(resource="dataset_import_job", **self._import_job_params)

    @property
    def arn(self):
        """
        Get the ARN of this resource
        :return: The ARN of this resource
        """
        history = self.history()
        if not history:
            return None

        import_arn = history[0].get("DatasetImportJobArn")
        return import_arn

    def history(self):
        """
        Get this dataset import job history from the Amazon Forecast Service
        :return: List of dataset import jobs for this dataset, in descending order by creation time
        """
        past_imports = []
        paginator = self.cli.get_paginator("list_dataset_import_jobs")
        iterator = paginator.paginate(
            Filters=[
                {"Condition": "IS", "Key": "DatasetArn", "Value": self.dataset_arn}
            ]
        )
        for page in iterator:
            past_imports.extend(page.get("DatasetImportJobs", []))

        past_imports = sorted(
            past_imports, key=itemgetter("LastModificationTime"), reverse=True
        )
        return past_imports

    @property
    def status(self) -> Status:
        """
        Get the status of this dataset import job
        :return: Status
        """
        previous_imports = self.history()

        # check if the data has not been imported
        if not previous_imports:
            return Status.DOES_NOT_EXIST

        # check if the data is outdated
        last_import_arn = previous_imports[0].get("DatasetImportJobArn")
        previous_status = self.cli.describe_dataset_import_job(
            DatasetImportJobArn=last_import_arn
        )

        # if the data is active, check if it should be updated
        if previous_status.get("Status") == Status.ACTIVE:
            past_etag = self.get_service_tag_for_arn(last_import_arn, "SolutionETag")

            # always re-import data if upgrading from 1.0 (adds the etag tag)
            # always re-import data if upgrading from 1.1 with a multipart etag (large datasets)
            if not past_etag:
                logger.info(
                    "no signature found for this dataset - marking as DOES_NOT_EXIST to trigger import"
                )
                return Status.DOES_NOT_EXIST  # re-import data to

            # always re-import the data if the signature has changed
            if past_etag != self.dataset_file.etag:
                logger.info(
                    "signature found for this dataset, but it does not match the current imported dataset signature - marking as DOES_NOT_EXIST to trigger import"
                )
                return Status.DOES_NOT_EXIST

        return Status[previous_status.get("Status")]

    def create(self):
        """
        Create the dataset import job
        :return: None
        """
        dataset_name = self.dataset_arn.split("/")[-1]
        now = self.dataset_file.last_updated.strftime("%Y_%m_%d_%H_%M_%S")
        name = f"{dataset_name}_{now}"
        self._import_job_params["DatasetImportJobName"] = name
        self.add_tag("SolutionETag", self.dataset_file.etag)
        self._import_job_params["Tags"] = self.tags

        try:
            self.cli.create_dataset_import_job(**self._import_job_params)
        except self.cli.exceptions.ResourceAlreadyExistsException:
            logger.debug("Dataset import job %s is already creating" % name)