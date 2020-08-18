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
from typing import Union

from shared.Dataset.dataset_file import DatasetFile
from shared.DatasetGroup.dataset_group import (
    DatasetGroup,
    LATEST_DATASET_UPDATE_FILENAME_TAG,
    LATEST_DATASET_UPDATE_FILE_ETAG_TAG,
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

    @property
    def status(self) -> Status:
        """
        Get the status of the predictor as defined. The status might be DOES_NOT_EXIST if a predictor of the desired
        format does not yet exist, or a predictor needs to be regenerated.
        :return: Status
        """

        # this ensures that only the last file uploaded will trigger predictor generation
        last_updated_file = self.get_service_tag_for_arn(
            self._dataset_group.arn, LATEST_DATASET_UPDATE_FILENAME_TAG
        )
        last_updated_etag = self.get_service_tag_for_arn(
            self._dataset_group.arn, LATEST_DATASET_UPDATE_FILE_ETAG_TAG
        )
        logger.debug(
            "last updated file was %s with ETag %s"
            % (last_updated_file, last_updated_etag)
        )
        if (
            self._dataset_file.filename == last_updated_file
            and self._dataset_file.etag == last_updated_etag
        ):
            logger.info(
                "this state machine execution can continue through predictor and forecast creation"
            )
        else:
            logger.info(
                "this state machine execution cannot continue through predictor and forecast creation"
            )
            raise NotMostRecentUpdate

        # check if we can actually update (dataset group is ready)
        # this raises exception DatasetsImporting if one or more datasets is importing
        self._dataset_group.ensure_ready()

        # check if a predictor has been created
        past_predictors = self.history()
        if not past_predictors:
            logger.debug("No past predictors found")
            return Status.DOES_NOT_EXIST

        logger.debug("predictor can update")
        past_status = self.cli.describe_predictor(
            PredictorArn=past_predictors[0].get("PredictorArn")
        )

        # regenerate the predictor if our past status was failed - this allows for the user to replace
        # a dataset with errors with a new dataset, and a new predictor to be generated.
        if Status[past_status.get("Status")].failed:
            logger.info(
                "a last predictor failed to create - will attempt to create again"
            )
            return Status.DOES_NOT_EXIST

        # regenerate the predictor if our predictor has aged out and we have new data
        last_modified = past_status.get("CreationTime")

        now = datetime.now(timezone.utc)
        max_age_s = self._max_age_s
        max_age_d = now - timedelta(seconds=max_age_s)

        if last_modified < max_age_d:
            logger.info(
                "predictor has surpassed max age - will attempt to create again"
            )
            return Status.DOES_NOT_EXIST

        # otherwise, return the actual status of the most recent predictor -
        # we do not have to regenerate it
        logger.info("predictor status is %s" % past_status.get("Status"))
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
            logger.debug("Predictor %s is already creating" % predictor_name)

    @property
    def latest_timestamp(self, format="%Y_%m_%d_%H_%M_%S"):
        past_predictors = self.history()
        latest_predictor_modified = max(
            [predictor.get("LastModificationTime") for predictor in past_predictors]
        )
        if format:
            return latest_predictor_modified.strftime(format)
        else:
            return latest_predictor_modified
