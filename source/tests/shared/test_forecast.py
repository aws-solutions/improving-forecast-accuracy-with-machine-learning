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

from datetime import datetime

import boto3
import pytest
from botocore.stub import Stubber
from moto import mock_sts

from shared.Dataset.dataset_file import DatasetFile
from shared.Forecast.forecast import Export
from shared.config import Config
from shared.status import Status


@pytest.fixture
def forecast_stub():
    client = boto3.client("forecast", region_name="us-east-1")
    with Stubber(client) as stubber:
        yield stubber


def test_default_export_status():
    assert Export().status == Status.DOES_NOT_EXIST


@mock_sts
def test_init_forecast(forecast_stub, configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTNPTS.csv", "some_bucket")
    forecast = config.forecast(dataset_file, "RetailDemandTNPTS")
    dataset_group = config.dataset_group(dataset_file)

    assert (
        forecast._dataset_group.dataset_group_name == dataset_group.dataset_group_name
    )
    assert forecast._forecast_config == config.config_item(dataset_file, "Forecast")


@mock_sts
def test_forecast_arn(forecast_stub, configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTNPTS.csv", "some_bucket")
    forecast = config.forecast(dataset_file, "RetailDemandTNPTS")

    forecast.cli = forecast_stub.client
    forecast_stub.add_response(
        "list_forecasts",
        {
            "Forecasts": [
                {
                    "LastModificationTime": datetime(2015, 1, 1),
                    "ForecastArn": "arn:2015-1-1",
                },
                {
                    "LastModificationTime": datetime(2017, 1, 1),
                    "ForecastArn": "arn:2017-1-1",
                },
            ]
        },
    )

    assert forecast.arn == "arn:2017-1-1"


@mock_sts
def test_forecast_history(forecast_stub, configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTNPTS.csv", "some_bucket")
    forecast = config.forecast(dataset_file, "RetailDemandTNPTS")

    forecast.cli = forecast_stub.client
    forecast_stub.add_response(
        "list_forecasts",
        {
            "Forecasts": [
                {
                    "LastModificationTime": datetime(2015, 1, 1),
                    "ForecastArn": "arn:2015-1-1",
                    "Status": "ACTIVE",
                },
                {
                    "LastModificationTime": datetime(2017, 1, 1),
                    "ForecastArn": "arn:2017-1-1",
                    "Status": "CREATE_IN_PROGRESS",
                },
            ]
        },
    )

    history = forecast.history()
    assert history[0].get("LastModificationTime") == datetime(2017, 1, 1)
    assert history[1].get("LastModificationTime") == datetime(2015, 1, 1)


@mock_sts
def test_status_not_yet_created(forecast_stub, configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTNPTS.csv", "some_bucket")
    forecast = config.forecast(dataset_file, "RetailDemandTNPTS")

    forecast.cli = forecast_stub.client
    forecast_stub.add_response("list_forecasts", {"Forecasts": []})

    assert forecast.status == Status.DOES_NOT_EXIST
    forecast_stub.assert_no_pending_responses()
