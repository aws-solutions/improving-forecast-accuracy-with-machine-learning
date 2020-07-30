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

from shared.Dataset.dataset_file import DatasetFile
from shared.DatasetGroup.dataset_group import DatasetGroup
from shared.helpers import ForecastClient
from shared.status import Status

MAX_AGE = 604800  # one week in seconds


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
    def arn(self) -> str:
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
    def can_update(self) -> bool:
        """
        Check if a predictor can update. Predictors can update if all of their Datasets have Status ACTIVE
        :return: true if the predictor can update, otherwise false
        """
        dataset_group = self.cli.describe_dataset_group(
            DatasetGroupArn=self._dataset_group.arn
        )

        datasets = [
            self.cli.describe_dataset(DatasetArn=arn)
            for arn in dataset_group.get("DatasetArns")
        ]

        datasets_ready = all(dataset.get("Status") == "ACTIVE" for dataset in datasets)
        if not datasets_ready:
            raise ValueError(
                f"One or more of the datasets for dataset group {dataset_group.get('DatasetGroupName')} are not ACTIVE"
            )

        return datasets_ready

    @property
    def status(self) -> Status:
        """
        Get the status of the predictor as defined. The status might be DOES_NOT_EXIST if a predictor of the desired
        format does not yet exist, or a predictor needs to be regenerated.
        :return: Status
        """
        past_predictors = self.history()

        # check if a predictor has been created
        if not past_predictors and self.can_update:
            return Status.DOES_NOT_EXIST

        # raises an exception if we can't update
        self.can_update

        past_status = self.cli.describe_predictor(
            PredictorArn=past_predictors[0].get("PredictorArn")
        )

        # regenerate the predictor if our past status was failed - this allows for the user to replace
        # a dataset with errors with a new dataset, and a new predictor to be generated.
        if Status[past_status.get("Status")].failed:
            return Status.DOES_NOT_EXIST

        # regenerate the predictor if our predictor has aged out and we have new data
        last_modified = past_status.get("CreationTime")

        now = datetime.now(timezone.utc)
        max_age_s = self._max_age_s
        max_age_d = now - timedelta(seconds=max_age_s)

        if last_modified < max_age_d:
            return Status.DOES_NOT_EXIST

        # otherwise, return the actual status of the most recent predictor -
        # we do not have to regenerate it
        return Status[past_status.get("Status")]

    def create(self):
        """
        Create this predictor
        :return: None
        """
        dataset_group_name = self._dataset_group.dataset_group_name
        now = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")

        self._predictor_params["PredictorName"] = f"{dataset_group_name}_{now}"

        self.cli.create_predictor(**self._predictor_params)
