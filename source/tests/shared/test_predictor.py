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

from datetime import datetime, timezone

import boto3
import pytest
from botocore.stub import Stubber
from moto import mock_sts

from shared.Dataset.dataset_file import DatasetFile
from shared.config import Config
from shared.status import Status


@pytest.fixture
def expected_dataset_arns():
    return [
        f"arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/RetailDemandTNPTS",
        f"arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/RetailDemandTNPTS_related",
        f"arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/RetailDemandTNPTS_metadata",
    ]


@pytest.fixture
def forecast_stub():
    client = boto3.client("forecast", region_name="us-east-1")
    with Stubber(client) as stubber:
        yield stubber


@mock_sts
def test_init_predictor(forecast_stub, configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTNPTS.csv", "some_bucket")
    predictor = config.predictor(dataset_file)

    predictor.cli = forecast_stub.client

    assert predictor._dataset_file == dataset_file
    for k, v in config.config_item(dataset_file, "Predictor").items():
        if k != "MaxAge":
            assert predictor._predictor_params.get(k) == v
    # assert predictor._predictor_config == config.config_item(dataset_file, "Predictor")


@mock_sts
def test_predictor_arn(forecast_stub, configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTNPTS.csv", "some_bucket")
    predictor = config.predictor(dataset_file)

    predictor.cli = forecast_stub.client
    forecast_stub.add_response(
        "list_predictors",
        {
            "Predictors": [
                {"CreationTime": datetime(2015, 1, 1), "PredictorArn": "arn:2015-1-1"},
                {"CreationTime": datetime(2017, 1, 1), "PredictorArn": "arn:2017-1-1"},
            ]
        },
    )

    assert predictor.arn == "arn:2017-1-1"


@mock_sts
def test_predictor_history(forecast_stub, configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTNPTS.csv", "some_bucket")
    predictor = config.predictor(dataset_file)

    predictor.cli = forecast_stub.client
    forecast_stub.add_response(
        "list_predictors",
        {
            "Predictors": [
                {
                    "CreationTime": datetime(2015, 1, 1),
                    "PredictorArn": "arn:2015-1-1",
                    "Status": "ACTIVE",
                },
                {
                    "CreationTime": datetime(2017, 1, 1),
                    "PredictorArn": "arn:2017-1-1",
                    "Status": "CREATE_IN_PROGRESS",
                },
            ]
        },
    )

    history = predictor.history()
    assert history[0].get("CreationTime") == datetime(2017, 1, 1)
    assert history[1].get("CreationTime") == datetime(2015, 1, 1)


@mock_sts
def test_can_update(forecast_stub, configuration_data, expected_dataset_arns):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTNPTS.csv", "some_bucket")
    predictor = config.predictor(dataset_file)

    predictor.cli = forecast_stub.client
    forecast_stub.add_response(
        "describe_dataset_group", {"DatasetArns": expected_dataset_arns}
    )
    for arn in expected_dataset_arns:
        forecast_stub.add_response("describe_dataset", {"Status": "ACTIVE"})

    assert predictor.can_update


@mock_sts
def test_cant_update(forecast_stub, configuration_data, expected_dataset_arns):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTNPTS.csv", "some_bucket")
    predictor = config.predictor(dataset_file)

    predictor.cli = forecast_stub.client
    forecast_stub.add_response(
        "describe_dataset_group", {"DatasetArns": expected_dataset_arns}
    )
    for arn in expected_dataset_arns:
        forecast_stub.add_response("describe_dataset", {"Status": "CREATE_PENDING"})

    with pytest.raises(ValueError):
        predictor.can_update


@mock_sts
def test_status_not_yet_created(
    forecast_stub, configuration_data, expected_dataset_arns
):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTNPTS.csv", "some_bucket")
    predictor = config.predictor(dataset_file)

    predictor.cli = forecast_stub.client
    forecast_stub.add_response("list_predictors", {"Predictors": []})
    forecast_stub.add_response(
        "describe_dataset_group", {"DatasetArns": expected_dataset_arns}
    )
    for arn in expected_dataset_arns:
        forecast_stub.add_response(
            "describe_dataset", {"Status": "ACTIVE", "DatasetArn": arn}
        )

    assert predictor.status == Status.DOES_NOT_EXIST
    forecast_stub.assert_no_pending_responses()


@mock_sts
def test_status_aged_out(forecast_stub, configuration_data, expected_dataset_arns):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTNPTS.csv", "some_bucket")
    predictor = config.predictor(dataset_file)

    predictor.cli = forecast_stub.client
    forecast_stub.add_response(
        "list_predictors",
        {
            "Predictors": [
                {
                    "PredictorArn": "arn:",
                    "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
                }
            ]
        },
    )
    forecast_stub.add_response(
        "describe_dataset_group", {"DatasetArns": expected_dataset_arns}
    )
    for arn in expected_dataset_arns:
        forecast_stub.add_response(
            "describe_dataset", {"Status": "ACTIVE", "DatasetArn": arn}
        )
    forecast_stub.add_response(
        "describe_predictor",
        {"CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc), "Status": "ACTIVE"},
    )

    assert predictor.status == Status.DOES_NOT_EXIST


@mock_sts
def test_status_still_good(forecast_stub, configuration_data, expected_dataset_arns):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTNPTS.csv", "some_bucket")
    predictor = config.predictor(dataset_file)

    predictor.cli = forecast_stub.client
    forecast_stub.add_response(
        "list_predictors",
        {
            "Predictors": [
                {"PredictorArn": "arn:", "CreationTime": datetime.now(timezone.utc)}
            ]
        },
    )
    forecast_stub.add_response(
        "describe_dataset_group", {"DatasetArns": expected_dataset_arns}
    )
    for arn in expected_dataset_arns:
        forecast_stub.add_response(
            "describe_dataset", {"Status": "ACTIVE", "DatasetArn": arn}
        )
    forecast_stub.add_response(
        "describe_predictor",
        {"CreationTime": datetime.now(timezone.utc), "Status": "ACTIVE"},
    )

    assert predictor.status == Status.ACTIVE
