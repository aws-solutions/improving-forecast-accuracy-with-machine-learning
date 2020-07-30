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

import copy
import os

import boto3
import pytest
import yaml
from botocore.exceptions import ClientError
from botocore.stub import Stubber
from moto import mock_sts

from shared.Dataset.dataset_domain import DatasetDomain
from shared.Dataset.dataset_file import DatasetFile
from shared.Dataset.dataset_type import DatasetType
from shared.config import Config, ConfigNotFound
from shared.status import Status


@pytest.fixture
def forecast_stub():
    client = boto3.client("forecast", region_name="us-east-1")
    with Stubber(client) as stubber:
        yield stubber


@mock_sts
def test_dataset_default(configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("some_new_key.csv", "some_bucket")

    ds = config.dataset(dataset_file)
    assert ds.data_frequency == "D"
    assert ds.dataset_type == DatasetType.TARGET_TIME_SERIES
    assert ds.dataset_domain == DatasetDomain.RETAIL
    assert ds.dataset_name == "some_new_key"
    assert ds.dataset_schema == {
        "Attributes": [
            {"AttributeName": "item_id", "AttributeType": "string"},
            {"AttributeName": "timestamp", "AttributeType": "timestamp",},
            {"AttributeName": "demand", "AttributeType": "float"},
        ]
    }


@pytest.fixture(scope="session")
def config_for(csv):
    with open(
        os.path.join(
            os.path.dirname(__file__), "fixtures", "config_and_overrides.yaml"
        ),
        "r",
    ) as f:
        body = f.read()
    config_dict = yaml.safe_load(body)

    config = Config()
    config.config = config_dict

    if not csv:
        csv = "some-default.csv"
    if ".csv" not in csv:
        csv = csv + ".csv"

    dataset_file = DatasetFile(csv, "some_bucket")
    return config


@mock_sts
def test_dataset_import_job_default(configuration_data, forecast_stub):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("some_new_key.csv", "some_bucket")

    dsij = config.dataset_import_job(dataset_file)
    assert (
        dsij.dataset_arn
        == "arn:aws:forecast:us-east-1:abcdefghijkl:dataset/some_new_key"
    )

    # the stubber needs to be initialized because the ARN needs dataset import job history
    dsij.cli = forecast_stub.client
    forecast_stub.add_response(
        method="list_dataset_import_jobs", service_response={"DatasetImportJobs": []}
    )

    assert not dsij.arn


@mock_sts
def test_dataset_group_default(configuration_data, forecast_stub):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("some_new_key.csv", "some_bucket")

    dsg = config.dataset_group(dataset_file=dataset_file)

    assert (
        dsg.arn == "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/some_new_key"
    )
    assert dsg.dataset_group_name == "some_new_key"
    assert dsg.dataset_group_domain == DatasetDomain.RETAIL

    dsg.cli = forecast_stub.client
    forecast_stub.add_client_error(
        "describe_dataset_group", "ResourceNotFoundException"
    )

    assert dsg.status == Status.DOES_NOT_EXIST


@mock_sts
def test_dataset_group_mismatch(configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("Mismatch.csv", "some_bucket")
    with pytest.raises(ValueError) as excinfo:
        config.dataset_group(dataset_file)

    assert "must match" in str(excinfo.value)


@mock_sts
def test_config_required_datasets(configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("some_new_key.csv", "some_bucket")

    assert config.required_datasets(dataset_file) == ["TARGET_TIME_SERIES"]


@mock_sts
def test_config_required_datasets_override(configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("Override.csv", "some_bucket")
    required_datasets = config.required_datasets(dataset_file)
    assert "TARGET_TIME_SERIES" in required_datasets
    assert "RELATED_TIME_SERIES" in required_datasets
    assert "ITEM_METADATA" in required_datasets


@mock_sts
def test_datasets(configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTRM.csv", "some_bucket")

    datasets = config.datasets(dataset_file)
    assert len(datasets) == 3

    for dataset in datasets:
        if dataset.dataset_type == DatasetType.TARGET_TIME_SERIES:
            assert dataset.dataset_name == "RetailDemandTRM"
        elif dataset.dataset_type == DatasetType.RELATED_TIME_SERIES:
            assert dataset.dataset_name == "RetailDemandTRM_related"
        elif dataset.dataset_type == DatasetType.ITEM_METADATA:
            assert dataset.dataset_name == "RetailDemandTRM_metadata"


def test_missing_timeseries(configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandForgottenDatasets.csv", "some_bucket")
    with pytest.raises(ValueError) as excinfo:
        config.required_datasets(dataset_file)

    assert "you must configure a TARGET_TIME_SERIES dataset" in str(excinfo.value)


def test_duplicate_timeseries(configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandDuplicateDatasets.csv", "some_bucket")
    with pytest.raises(ValueError) as excinfo:
        config.required_datasets(dataset_file)

    assert "duplicate dataset types" in str(excinfo.value)


def test_from_sfn(configuration_data):
    config = Config.from_sfn(event={"config": configuration_data})

    assert config.config == configuration_data


def test_from_s3(s3_valid_config, configuration_data):
    config = Config.from_s3(bucket="testbucket")

    assert config.config == configuration_data


def test_from_s3_bucket_missing(s3_valid_config):
    config = Config()

    with pytest.raises(ClientError) as error:
        config.from_s3(bucket="doesntexist")

    assert error.value.response.get("Error").get("Code") == "NoSuchBucket"


def test_from_s3_missing_config(s3_missing_config):
    config = Config()

    with pytest.raises(ConfigNotFound) as error:
        config.from_s3(bucket="testbucket")


def test_from_s3_exceptions_is_actually_str(s3_valid_config):
    s3_valid_config.put_object(
        Bucket="testbucket", Key="forecast-defaults.yaml", Body="this_is_yaml"
    )

    config = Config()
    with pytest.raises(ValueError) as excinfo:
        config.from_s3(bucket="testbucket")

    assert "should contain a YAML dict" in str(excinfo.value)


def test_from_s3_exceptions_missing_defaults(s3_valid_config):
    contents = """
Defualts: 
  Look: I misspelled 'Defaults' 
    """

    s3_valid_config.put_object(
        Bucket="testbucket", Key="forecast-defaults.yaml", Body=contents
    )

    config = Config()
    with pytest.raises(ValueError) as excinfo:
        config.from_s3(bucket="testbucket")

    assert "hould contain a `Default` key" in str(excinfo.value)


def test_from_s3_exceptions_missing_defaults(s3_valid_config):
    contents = """
Defualts: 
  Look: I misspelled 'Defaults' 
    """

    s3_valid_config.put_object(
        Bucket="testbucket", Key="forecast-defaults.yaml", Body=contents
    )

    config = Config()
    with pytest.raises(ValueError) as excinfo:
        config.from_s3(bucket="testbucket")

    assert "should contain a `Default` key" in str(excinfo.value)


def test_from_s3_exceptions_malformed(s3_valid_config):
    contents = "Why: Would you do this to me::"

    s3_valid_config.put_object(
        Bucket="testbucket", Key="forecast-defaults.yaml", Body=contents
    )

    config = Config()
    with pytest.raises(ValueError) as excinfo:
        config.from_s3(bucket="testbucket")

    assert "is not a valid config file" in str(excinfo.value)


@mock_sts
def test_config_valid(configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("some_new_key.csv", "some_bucket")
    errors = config.validate()
    assert not errors


def test_config_validation_doesnt_mutate_config(configuration_data):
    config = Config()
    config.config = configuration_data

    config_copy = copy.deepcopy(configuration_data)
    config.validate()

    assert config.config == config_copy
