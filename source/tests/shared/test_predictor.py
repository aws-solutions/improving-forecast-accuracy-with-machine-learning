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
from moto import mock_sts, mock_s3

from shared.Dataset.dataset_file import DatasetFile
from shared.DatasetGroup.dataset_group import (
    LATEST_DATASET_UPDATE_FILENAME_TAG,
    LATEST_DATASET_UPDATE_FILE_ETAG_TAG,
)
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


@pytest.fixture
def mocked_predictor(configuration_data):
    bucket_name = "some_bucket"
    key = "train/RetailDemandTNPTS.csv"
    with mock_sts():
        with mock_s3():
            cli = boto3.client("s3", region_name="us-east-1")
            cli.create_bucket(Bucket=bucket_name)
            cli.put_object(
                Bucket=bucket_name, Key=key, Body=f"some-contents",
            )

            config = Config()
            config.config = configuration_data
            dataset_file = DatasetFile(key, bucket_name)
            predictor = config.predictor(dataset_file)
            yield predictor


def test_predictor_status(mocked_predictor, mocker):
    def service_tags(*args):
        if args[1] == LATEST_DATASET_UPDATE_FILENAME_TAG:
            return "RetailDemandTNPTS.csv"
        if args[1] == LATEST_DATASET_UPDATE_FILE_ETAG_TAG:
            return "0b9791ad102b5f5f06ef68cef2aae26e"

    mocked_predictor.get_service_tag_for_arn = mocker.MagicMock()
    mocked_predictor.get_service_tag_for_arn.side_effect = service_tags

    mocked_predictor._dataset_group.ensure_ready = mocker.MagicMock()
    mocked_predictor.history = mocker.MagicMock()
    mocked_predictor.history.return_value = []

    assert mocked_predictor.status == Status.DOES_NOT_EXIST


def test_predictor_status_last_failed(mocked_predictor, mocker):
    def service_tags(*args):
        if args[1] == LATEST_DATASET_UPDATE_FILENAME_TAG:
            return "RetailDemandTNPTS.csv"
        if args[1] == LATEST_DATASET_UPDATE_FILE_ETAG_TAG:
            return "0b9791ad102b5f5f06ef68cef2aae26e"

    mocked_predictor.get_service_tag_for_arn = mocker.MagicMock()
    mocked_predictor.get_service_tag_for_arn.side_effect = service_tags

    mocked_predictor._dataset_group.ensure_ready = mocker.MagicMock()
    mocked_predictor.history = mocker.MagicMock()
    mocked_predictor.history.return_value = [{"PredictorArn": "mocked"}]
    mocked_predictor.cli.describe_predictor = mocker.MagicMock()
    mocked_predictor.cli.describe_predictor.return_value = {
        "Status": str(Status.CREATE_FAILED)
    }

    assert mocked_predictor.status == Status.DOES_NOT_EXIST


def test_predictor_status_last_failed(mocked_predictor, mocker):
    def service_tags(*args):
        if args[1] == LATEST_DATASET_UPDATE_FILENAME_TAG:
            return "RetailDemandTNPTS.csv"
        if args[1] == LATEST_DATASET_UPDATE_FILE_ETAG_TAG:
            return "0b9791ad102b5f5f06ef68cef2aae26e"

    mocked_predictor.get_service_tag_for_arn = mocker.MagicMock()
    mocked_predictor.get_service_tag_for_arn.side_effect = service_tags

    mocked_predictor._dataset_group.ensure_ready = mocker.MagicMock()
    mocked_predictor.history = mocker.MagicMock()
    mocked_predictor.history.return_value = [{"PredictorArn": "mocked"}]
    mocked_predictor.cli.describe_predictor = mocker.MagicMock()
    mocked_predictor.cli.describe_predictor.return_value = {
        "Status": str(Status.ACTIVE),
        "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    }

    assert mocked_predictor.status == Status.DOES_NOT_EXIST


def test_predictor_aged_out(mocked_predictor, mocker):
    def service_tags(*args):
        if args[1] == LATEST_DATASET_UPDATE_FILENAME_TAG:
            return "RetailDemandTNPTS.csv"
        if args[1] == LATEST_DATASET_UPDATE_FILE_ETAG_TAG:
            return "0b9791ad102b5f5f06ef68cef2aae26e"

    mocked_predictor.get_service_tag_for_arn = mocker.MagicMock()
    mocked_predictor.get_service_tag_for_arn.side_effect = service_tags

    mocked_predictor._dataset_group.ensure_ready = mocker.MagicMock()
    mocked_predictor.history = mocker.MagicMock()
    mocked_predictor.history.return_value = [{"PredictorArn": "mocked"}]
    mocked_predictor.cli.describe_predictor = mocker.MagicMock()
    mocked_predictor.cli.describe_predictor.return_value = {
        "Status": str(Status.ACTIVE),
        "CreationTime": datetime(2015, 1, 1, tzinfo=timezone.utc),
    }

    assert mocked_predictor.status == Status.DOES_NOT_EXIST


def test_predictor_active(mocked_predictor, mocker):
    def service_tags(*args):
        if args[1] == LATEST_DATASET_UPDATE_FILENAME_TAG:
            return "RetailDemandTNPTS.csv"
        if args[1] == LATEST_DATASET_UPDATE_FILE_ETAG_TAG:
            return "0b9791ad102b5f5f06ef68cef2aae26e"

    mocked_predictor.get_service_tag_for_arn = mocker.MagicMock()
    mocked_predictor.get_service_tag_for_arn.side_effect = service_tags

    mocked_predictor._dataset_group.ensure_ready = mocker.MagicMock()
    mocked_predictor.history = mocker.MagicMock()
    mocked_predictor.history.return_value = [{"PredictorArn": "mocked"}]
    mocked_predictor.cli.describe_predictor = mocker.MagicMock()
    mocked_predictor.cli.describe_predictor.return_value = {
        "Status": str(Status.ACTIVE),
        "CreationTime": datetime.now(timezone.utc),
    }

    assert mocked_predictor.status == Status.ACTIVE


def test_predictor_create_valid(mocked_predictor, mocker):
    mocked_predictor.cli = mocker.MagicMock()
    mocked_predictor.cli.exceptions = mocker.MagicMock()
    mocked_predictor._dataset_group = mocker.MagicMock()
    mocked_predictor._dataset_group.latest_timestamp.return_value = (
        "2015_01_01_00_00_00"
    )

    mocked_predictor.create()
    assert mocked_predictor.cli.create_predictor.called_once
