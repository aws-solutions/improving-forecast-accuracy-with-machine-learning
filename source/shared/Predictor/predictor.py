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

from datetime import datetime, timezone, timedelta
from operator import itemgetter
from typing import Union, Dict

from shared.Dataset.dataset_file import DatasetFile
from shared.DatasetGroup.dataset_group import (
    DatasetGroup,
    LATEST_DATASET_UPDATE_FILENAME_TAG,
)
from shared.helpers import ForecastClient
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


class Predictor(ForecastClient):
    def __init__(
        self, dataset_file: DatasetFile, dataset_group: DatasetGroup, **predictor_config
    ):
        self._dataset_file = dataset_file
        self._dataset_group = dataset_group
        self._max_age_s = predictor_config.pop("MaxAge", MAX_AGE)

        self._predictor_params = {
            "PredictorName": "PLACEHOLDER",
            "InputDataConfig": {"DatasetGroupArn": self._dataset_group.arn},
            **predictor_config,
        }

        super().__init__(resource="predictor", **self._predictor_params)

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

        paginator = self.cli.get_paginator("list_predictors")
        iterator = paginator.paginate(Filters=filters)
        for page in iterator:
            past_predictors.extend(page.get("Predictors", []))

        past_predictors = sorted(
            past_predictors, key=itemgetter("CreationTime"), reverse=True
        )
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
        last_predictor = self.cli.describe_predictor(
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

        # check if a predictor has been successfully created previously
        too_old = self._status_predictor_too_old(past_status)
        if too_old:
            return Status.DOES_NOT_EXIST

        logger.info("status check: predictor status is %s" % past_status.get("Status"))
        return Status[past_status.get("Status")]

    def create(self):
        """
        Create this predictor
        :return: None
        """
        dataset_group_name = self._dataset_group.dataset_group_name
        latest_dataset_update = self._dataset_group.latest_timestamp
        predictor_name = f"{dataset_group_name}_{latest_dataset_update}"

        self._predictor_params["PredictorName"] = predictor_name
        self._predictor_params["Tags"] = self.tags

        try:
            self.cli.create_predictor(**self._predictor_params)
        except self.cli.exceptions.ResourceAlreadyExistsException:
            logger.debug(
                "Predictor %s is already creating, or already exists" % predictor_name
            )

    def latest_timestamp(self, format="%Y_%m_%d_%H_%M_%S"):
        past_predictors = self.history()
        latest_predictor_modified = max(
            [predictor.get("LastModificationTime") for predictor in past_predictors]
        )
        if format:
            return latest_predictor_modified.strftime(format)
        else:
            return latest_predictor_modified
