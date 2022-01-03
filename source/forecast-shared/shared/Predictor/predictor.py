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

from datetime import datetime, timezone, timedelta
from operator import itemgetter
from os import environ
from typing import Union, Dict, Optional

from shared.Dataset.dataset_file import DatasetFile
from shared.DatasetGroup.dataset_group import (
    DatasetGroup,
    LATEST_DATASET_UPDATE_FILENAME_TAG,
)
from shared.helpers import ForecastClient, UserTags
from shared.logging import get_logger
from shared.status import Status

MAX_AGE = 604800  # one week in seconds
logger = get_logger(__name__)


class NotMostRecentUpdate(Exception):
    """
    This is raised when a predictor status is requested for a run in an execution that was not triggered by the most
    recent file update.
    """

    pass


class Export:
    """Used to hold the status of an Amazon Forecast predictor backtest export"""

    status = Status.DOES_NOT_EXIST


class Predictor(ForecastClient):
    is_auto_predictor = False

    def __init__(
        self,
        dataset_file: DatasetFile,
        dataset_group: DatasetGroup,
        user_tags: Optional[UserTags] = None,
        **predictor_config,
    ):
        self._dataset_file = dataset_file
        self._dataset_group = dataset_group
        self._max_age_s = predictor_config.pop("MaxAge", MAX_AGE)

        if self.is_auto_predictor:
            input_data_config_key = "DataConfig"
        else:
            input_data_config_key = "InputDataConfig"

        logger.info(f"PREDICTOR CONFIG KEY {input_data_config_key}: {predictor_config}")

        # set any InputDataConfig items
        self._input_data_config = predictor_config.get(input_data_config_key, {})
        self._input_data_config["DatasetGroupArn"] = self._dataset_group.arn

        self._predictor_params = {
            "PredictorName": "PLACEHOLDER",
            input_data_config_key: self._input_data_config,
            **predictor_config,
        }

        if self.is_auto_predictor:
            super().__init__(
                resource="auto_predictor", user_tags=user_tags, **self._predictor_params
            )
        else:
            super().__init__(
                resource="predictor", user_tags=user_tags, **self._predictor_params
            )

    def _list_predictors(self, filters=None):
        paginator = self.cli.get_paginator("list_predictors")
        if filters:
            kwargs = {"Filters": filters}
        else:
            kwargs = {}
        iterator = paginator.paginate(**kwargs)
        past_predictors = []
        for page in iterator:
            past_predictors.extend(
                [
                    p
                    for p in page.get("Predictors", [])
                    if p.get("IsAutoPredictor") == self.is_auto_predictor
                ]
            )

        past_predictors = sorted(
            past_predictors, key=itemgetter("CreationTime"), reverse=True
        )
        return past_predictors

    def _describe_predictor(self, **kwargs):
        return (
            self.cli.describe_auto_predictor(**kwargs)
            if self.is_auto_predictor
            else self.cli.describe_predictor(**kwargs)
        )

    def _create_predictor(self, **kwargs):
        if self.is_auto_predictor:
            return self.cli.create_auto_predictor(**kwargs)
        else:
            return self.cli.create_predictor(**kwargs)

    @property
    def arn(self) -> Union[str, None]:
        """Get the ARN of this resource
        :return: The ARN of this resource if it exists, otherwise None
        """
        past_predictors = self.history()
        if not past_predictors:
            return None

        return past_predictors[0].get("PredictorArn")

    def history(self, status: Status = None):
        """
        Get this Predictor history from the Amazon Forecast service.
        :param status: The Status of the predictor(s) to return
        :return: List of past predictors, in descending order by creation time
        """
        past_predictors = []
        filters = [
            {
                "Condition": "IS",
                "Key": "DatasetGroupArn",
                "Value": self._dataset_group.arn,
            }
        ]

        if status:
            filters.append({"Condition": "IS", "Key": "Status", "Value": str(status)})

        past_predictors = self._list_predictors(filters)
        return past_predictors

    def _status_most_recent_update(self):
        last_updated_file = self.get_service_tag_for_arn(
            self._dataset_group.arn, LATEST_DATASET_UPDATE_FILENAME_TAG
        )
        logger.debug(
            "status check: triggered by file %s, latest update was %s"
            % (self._dataset_file.filename, last_updated_file)
        )
        if self._dataset_file.filename == last_updated_file:
            return True
        else:
            return False

    def _status_last_predictor(self) -> Union[None, Dict]:
        past_predictors = self.history()
        if not past_predictors:
            logger.debug("status check: no past predictors found")
            return None

        logger.debug("status check: previous predictor was found")
        last_predictor = self._describe_predictor(
            PredictorArn=past_predictors[0].get("PredictorArn")
        )

        if Status[last_predictor.get("Status")].failed:
            logger.info(
                "status check: previous predictor has failed status - attempt to recreate"
            )
            return None

        return last_predictor

    def _status_predictor_too_old(self, past_status: Dict) -> bool:
        last_modified = past_status.get("LastModificationTime")

        # check if (at least one of) the dataset files in this update are newer than the last predictor modification time
        datasets = self._dataset_group.datasets
        datasets_updated = False
        for dataset in datasets:
            dataset_last_modified = dataset.get("LastModificationTime")
            if dataset_last_modified > last_modified:
                datasets_updated = True
                logger.debug("status check: dataset %s newer than predictor")

        if not datasets_updated:
            logger.warning(
                "status check: no relevant dataset updates detected - did you mean to add new data?"
            )
            return False

        # check if the new dataset updates should trigger a predictor update
        now = datetime.now(timezone.utc)
        max_age_s = self._max_age_s
        max_age_d = now - timedelta(seconds=max_age_s)

        # we only have to check the max age if the data has actually changed within the window
        if last_modified < max_age_d:
            logger.info(
                "status check: predictor has surpassed max allowed age of %s seconds",
                max_age_s,
            )
            return True
        else:
            return False

    @property
    def status(self) -> Status:
        """
        Get the status of the predictor as defined. The status might be DOES_NOT_EXIST if a predictor of the desired
        format does not yet exist, or a predictor needs to be regenerated.
        :return: Status
        """

        # this ensures that only the last file uploaded will trigger predictor generation
        if not self._status_most_recent_update():
            raise NotMostRecentUpdate

        # check if dataset group is ready (all datasets are imported)
        # this raises exception DatasetsImporting if one or more datasets is importing
        dataset_group_ready = self._dataset_group.ready()
        if dataset_group_ready:
            logger.info("status check: all datasets have been successfully imported")

        past_status = self._status_last_predictor()
        if not past_status:
            return Status.DOES_NOT_EXIST

        # if the predictor is too old (and there is new data to train on), we return Status.DOES_NOT_EXIST to retrain
        too_old = self._status_predictor_too_old(past_status)
        if too_old:
            return Status.DOES_NOT_EXIST

        logger.info("status check: predictor status is %s" % past_status.get("Status"))
        self.set_user_tags(resource_arn=past_status["PredictorArn"])
        return Status[past_status.get("Status")]

    def _create_params(self):
        """
        Append tags and EncryptionConfig to the parameters to pass to CreatePredictor
        :return: the creation parameters
        """
        forecast_role = environ.get("FORECAST_ROLE", None)
        forecast_kms = environ.get("FORECAST_KMS", None)
        if forecast_role and forecast_kms:
            self._predictor_params["EncryptionConfig"] = {
                "KMSKeyArn": forecast_kms,
                "RoleArn": forecast_role,
            }
        return self._predictor_params

    def create(self):
        """
        Create this predictor
        :return: None
        """
        dataset_group_name = self._dataset_group.dataset_group_name
        latest_dataset_update = self._dataset_group.latest_timestamp
        if self.is_auto_predictor:
            predictor_name = f"{dataset_group_name}_auto_{latest_dataset_update}"
        else:
            predictor_name = f"{dataset_group_name}_{latest_dataset_update}"

        self._predictor_params["PredictorName"] = predictor_name
        self._predictor_params["Tags"] = self.tags

        try:
            # try to upgrade an autopredictor if an arn can be resolved
            if self.is_auto_predictor and self.arn:
                self._create_predictor(
                    PredictorName=predictor_name,
                    ReferencePredictorArn=self.arn,
                )
            else:
                self._create_predictor(**self._create_params())
        except self.cli.exceptions.ResourceAlreadyExistsException:
            logger.debug(
                "Predictor %s is already creating, or already exists" % predictor_name
            )

    def _latest_timestamp(self, format="%Y_%m_%d_%H_%M_%S"):
        """
        Predictors latest timestamp will be their creation date.
        :return:
        """
        past_predictors = self.history()
        latest_predictor_created = max(
            [predictor.get("CreationTime") for predictor in past_predictors]
        )
        if format:
            return latest_predictor_created.strftime(format)
        else:
            return latest_predictor_created

    def export(self, dataset_file: DatasetFile) -> Export:
        """
        Export/ check on a predictor backtest import
        :param dataset_file: The dataset file last updated that generated this predictor
        :return: Status
        """
        if not self.arn:
            raise ValueError(
                "Predictor does not yet exist - cannot perform backtest export."
            )

        # Build the export arn
        export_arn = self.arn.replace(":predictor/", ":predictor-backtest-export-job/")
        if self.is_auto_predictor:
            export_name = f"export_{self._dataset_group.dataset_group_name}_auto_{self._latest_timestamp()}"
            export_arn = export_arn[:-27]
        else:
            export_name = f"export_{self._dataset_group.dataset_group_name}_{self._latest_timestamp()}"
        export_arn = export_arn + f"/{export_name}"

        past_export = Export()
        try:
            past_status = self.cli.describe_predictor_backtest_export_job(
                PredictorBacktestExportJobArn=export_arn
            )
            past_export.status = Status[past_status.get("Status")]
        except self.cli.exceptions.ResourceInUseException as excinfo:
            logger.debug(
                "Predictor backtest export %s is updating: %s"
                % (export_name, str(excinfo))
            )
        except self.cli.exceptions.ResourceNotFoundException:
            logger.info("Creating predictor backtest export %s" % export_name)
            self.cli.create_predictor_backtest_export_job(
                PredictorArn=self.arn,
                PredictorBacktestExportJobName=export_name,
                Destination={
                    "S3Config": {
                        "Path": f"s3://{dataset_file.bucket}/exports/{export_name}",
                        "RoleArn": environ.get("FORECAST_ROLE"),
                    }
                },
            )
            past_export.status = Status.CREATE_PENDING

        logger.debug(
            "Predictor backtest export status for %s is %s"
            % (export_name, str(past_export.status))
        )
        self.set_user_tags(resource_arn=export_arn)
        return past_export
