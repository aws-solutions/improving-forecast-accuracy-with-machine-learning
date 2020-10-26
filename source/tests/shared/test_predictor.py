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
from dateutil.relativedelta import relativedelta
from moto import mock_sts, mock_s3

from shared.Dataset.dataset_file import DatasetFile
from shared.DatasetGroup.dataset_group import LATEST_DATASET_UPDATE_FILENAME_TAG
from shared.Predictor.predictor import NotMostRecentUpdate
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
    predictor = config.predictor(dataset_file, "RetailDemandTNPTS")

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
    predictor = config.predictor(dataset_file, "RetailDemandTNPTS")

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
    predictor = config.predictor(dataset_file, "RetailDemandTNPTS")

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
            predictor = config.predictor(dataset_file, "RetailDemandTNPTS")
            yield predictor


def test_predictor_status(mocked_predictor, mocker):
    def service_tags(*args):
        if args[1] == LATEST_DATASET_UPDATE_FILENAME_TAG:
            return "RetailDemandTNPTS.csv"

    mocked_predictor.get_service_tag_for_arn = mocker.MagicMock()
    mocked_predictor.get_service_tag_for_arn.side_effect = service_tags

    mocked_predictor._dataset_group.ready = mocker.MagicMock()
    mocked_predictor.history = mocker.MagicMock()
    mocked_predictor.history.return_value = []

    assert mocked_predictor.status == Status.DOES_NOT_EXIST


def test_predictor_status_not_most_recent(mocked_predictor, mocker):
    mocked_predictor._status_most_recent_update = mocker.MagicMock(return_value=False)
    with pytest.raises(NotMostRecentUpdate):
        mocked_predictor.status == Status.NOT_READY  # will never resolve a status


def test_predictor_status_no_predictor_or_old_failure(mocked_predictor, mocker):
    mocked_predictor._status_most_recent_update = mocker.MagicMock(return_value=True)
    mocked_predictor._dataset_group.ready = mocker.MagicMock(return_value=True)
    mocked_predictor._status_last_predictor = mocker.MagicMock(return_value=None)

    assert mocked_predictor.status == Status.DOES_NOT_EXIST


def test_predictor_status_too_old(mocked_predictor, mocker):
    mocked_predictor._status_most_recent_update = mocker.MagicMock(return_value=True)
    mocked_predictor._dataset_group.ready = mocker.MagicMock(return_value=True)
    mocked_predictor._status_last_predictor = mocker.MagicMock(return_value=None)
    mocked_predictor._status_predictor_too_old = mocker.MagicMock(return_value=True)

    assert mocked_predictor.status == Status.DOES_NOT_EXIST


def test_predictor_not_most_recent_status(mocked_predictor, mocker):
    mocked_predictor._status_most_recent_update = mocker.MagicMock(return_value=True)
    mocked_predictor._dataset_group.ready = mocker.MagicMock(return_value=True)
    mocked_predictor._status_last_predictor = mocker.MagicMock(
        return_value={"Status": "ACTIVE"}
    )
    mocked_predictor._status_predictor_too_old = mocker.MagicMock(return_value=False)

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


@pytest.mark.parametrize(
    "format,expected",
    [("%Y_%m_%d_%H_%M_%S", "2002_01_01_00_00_00"), (None, datetime(2002, 1, 1))],
    ids=["default", "datetime"],
)
def test_predictor_latest_timestamp(format, expected, mocked_predictor, mocker):
    mocked_predictor.history = mocker.MagicMock()
    mocked_predictor.history.return_value = [
        {"LastModificationTime": datetime(1999, 1, 1)},
        {"LastModificationTime": datetime(2002, 1, 1)},
        {"LastModificationTime": datetime(2000, 1, 1)},
        {"LastModificationTime": datetime(1998, 1, 1)},
    ]

    assert mocked_predictor.latest_timestamp(format=format) == expected


@pytest.mark.parametrize(
    "uploaded",
    [
        "RetailDemandTNPTS.csv",
        "RetailDemandTNPTS.related.csv",
        "RetailDemandTNPTS.metadata.csv",
    ],
)
def test_status_most_recent_update(mocked_predictor, uploaded, mocker):
    most_recent_update = "test.csv"
    service_tag_mock = mocker.MagicMock(return_value="most_recent_update")
    mocked_predictor.get_service_tag_for_arn = service_tag_mock

    result = mocked_predictor._status_most_recent_update()
    if uploaded == most_recent_update:
        assert result
    else:
        assert not result


def test_status_last_predictor_none(mocked_predictor, mocker):
    history = []
    history_mock = mocker.MagicMock(return_value=history)
    mocked_predictor.history = history_mock

    assert not mocked_predictor._status_last_predictor()


@pytest.mark.parametrize(
    "status", [Status.CREATE_FAILED, Status.DELETE_FAILED, Status.UPDATE_FAILED,]
)
def test_status_last_predictor_failed(mocked_predictor, mocker, status):
    predictor_description = {"PredictorArn": "some::arn", "Status": str(status)}
    history_mock = mocker.MagicMock(return_value=[predictor_description])
    cli_mock = mocker.MagicMock()
    cli_mock.describe_predictor.return_value = predictor_description
    mocked_predictor.history = history_mock
    mocked_predictor.cli = cli_mock

    assert not mocked_predictor._status_last_predictor()


def test_status_last_predictor_valid(mocked_predictor, mocker):
    predictor_description = {"PredictorArn": "some::arn", "Status": "ACTIVE"}
    history_mock = mocker.MagicMock(return_value=[predictor_description])
    cli_mock = mocker.MagicMock()
    cli_mock.describe_predictor.return_value = predictor_description
    mocked_predictor.history = history_mock
    mocked_predictor.cli = cli_mock

    assert mocked_predictor._status_last_predictor().get("Status") == "ACTIVE"


def test_status_too_old_not_updated(mocked_predictor, mocker):
    past_status = {"LastModificationTime": datetime(2000, 1, 1, tzinfo=timezone.utc)}
    datasets = [
        {"LastModificationTime": datetime(1990, 1, 1, tzinfo=timezone.utc)},
        {"LastModificationTime": datetime(1991, 1, 1, tzinfo=timezone.utc)},
        {"LastModificationTime": datetime(1992, 1, 1, tzinfo=timezone.utc)},
    ]
    mocked_predictor._dataset_group = mocker.MagicMock()
    type(mocked_predictor._dataset_group).datasets = mocker.PropertyMock(
        return_value=datasets
    )

    assert not mocked_predictor._status_predictor_too_old(past_status)


def test_status_in_interval(mocked_predictor, mocker):
    now = datetime.now(tz=timezone.utc)
    past_status = {"LastModificationTime": now}

    datasets = [
        {"LastModificationTime": now + relativedelta(days=+1)},
        {"LastModificationTime": now + relativedelta(days=+5)},
        {"LastModificationTime": now + relativedelta(days=+6)},
    ]
    mocked_predictor._dataset_group = mocker.MagicMock()
    type(mocked_predictor._dataset_group).datasets = mocker.PropertyMock(
        return_value=datasets
    )

    assert not mocked_predictor._status_predictor_too_old(past_status)


def test_status_in_interval(mocked_predictor, mocker):
    now = datetime.now(tz=timezone.utc)
    past_status = {"LastModificationTime": now - relativedelta(days=+7)}
    datasets = [
        {"LastModificationTime": now + relativedelta(days=+8)},
        {"LastModificationTime": now + relativedelta(days=+5)},
        {"LastModificationTime": now + relativedelta(days=+6)},
    ]
    mocked_predictor._max_age_s = 60 * 60 * 24 * 7
    mocked_predictor._dataset_group = mocker.MagicMock()
    type(mocked_predictor._dataset_group).datasets = mocker.PropertyMock(
        return_value=datasets
    )

    assert mocked_predictor._status_predictor_too_old(past_status)
