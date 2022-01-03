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

from datetime import datetime, timezone
from os import environ

import boto3
import pytest
from botocore.stub import Stubber
from dateutil.relativedelta import relativedelta
from moto import mock_sts, mock_s3

from shared.Dataset.dataset_file import DatasetFile
from shared.DatasetGroup.dataset_group import LATEST_DATASET_UPDATE_FILENAME_TAG
from shared.Predictor.predictor import NotMostRecentUpdate, Predictor
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


@pytest.fixture
def kms_enabled():
    environ.update(
        {
            "FORECAST_ROLE": "role",
            "FORECAST_KMS": "kms",
        }
    )
    yield
    environ.pop("FORECAST_ROLE")
    environ.pop("FORECAST_KMS")


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
                {
                    "CreationTime": datetime(2015, 1, 1),
                    "PredictorArn": "arn:2015-1-1",
                    "IsAutoPredictor": False,
                },
                {
                    "CreationTime": datetime(2017, 1, 1),
                    "PredictorArn": "arn:2017-1-1",
                    "IsAutoPredictor": False,
                },
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
                    "IsAutoPredictor": False,
                },
                {
                    "CreationTime": datetime(2017, 1, 1),
                    "PredictorArn": "arn:2017-1-1",
                    "Status": "CREATE_IN_PROGRESS",
                    "IsAutoPredictor": False,
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
                Bucket=bucket_name,
                Key=key,
                Body=f"some-contents",
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
        return_value={"Status": "ACTIVE", "PredictorArn": "arn:some"}
    )
    mocked_predictor.cli = mocker.MagicMock()
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
        {"CreationTime": datetime(1999, 1, 1)},
        {"CreationTime": datetime(2002, 1, 1)},
        {"CreationTime": datetime(2000, 1, 1)},
        {"CreationTime": datetime(1998, 1, 1)},
    ]

    assert mocked_predictor._latest_timestamp(format) == expected


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
    "status",
    [
        Status.CREATE_FAILED,
        Status.DELETE_FAILED,
        Status.UPDATE_FAILED,
    ],
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


@mock_sts
@pytest.mark.parametrize(
    "predictor_config",
    [
        {
            "InputDataConfig": {
                "SupplementaryFeatures": [
                    {
                        "Name": "holiday",
                        "Value": "CA",
                    }
                ]
            }
        },
        {
            "InputDataConfig": {
                "SupplementaryFeatures": [
                    {
                        "Name": "weather",
                        "Value": "CA",
                    }
                ]
            }
        },
        {
            "InputDataConfig": {
                "SupplementaryFeatures": [
                    {
                        "Name": "holiday",
                        "Value": "CA",
                    },
                    {
                        "Name": "weather",
                        "Value": "CA",
                    },
                ]
            }
        },
        {},
    ],
    ids=["holidays", "weather", "both", "none"],
)
def test_input_data_config_override(mocker, predictor_config):
    predictor = Predictor(
        mocker.MagicMock(),
        mocker.MagicMock(),
        **predictor_config,
    )

    if predictor_config:
        assert predictor._input_data_config[
            "SupplementaryFeatures"
        ] == predictor_config.get("InputDataConfig").get("SupplementaryFeatures")
    else:
        assert predictor._input_data_config.get("SupplementaryFeatures", None) == None
    assert predictor._input_data_config["DatasetGroupArn"]


@mock_sts
def test_create_params_encryption(configuration_data, kms_enabled):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTRM.csv", "some_bucket")
    dataset = config.dataset(dataset_file)

    create_params = dataset._create_params()
    assert "EncryptionConfig" in create_params.keys()
    assert create_params["EncryptionConfig"]["KMSKeyArn"] == "kms"
    assert create_params["EncryptionConfig"]["RoleArn"] == "role"


@mock_sts
def test_create_params_no_encryption(configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTRM.csv", "some_bucket")
    dataset = config.dataset(dataset_file)

    create_params = dataset._create_params()
    assert "EncryptionConfig" not in create_params.keys()
