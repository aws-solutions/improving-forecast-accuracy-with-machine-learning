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

from copy import deepcopy
from datetime import datetime

import boto3
import pytest
from botocore.stub import Stubber
from moto import mock_sts

from shared.Dataset.data_frequency import DataFrequency
from shared.Dataset.dataset_domain import DatasetDomain
from shared.Dataset.dataset_file import DatasetFile
from shared.Dataset.dataset_type import DatasetType
from shared.config import Config
from shared.status import Status


@pytest.fixture
def forecast_stub():
    client = boto3.client("forecast", region_name="us-east-1")
    with Stubber(client) as stubber:
        yield stubber


@mock_sts
def test_dataset_status_lifecycle(configuration_data, forecast_stub):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTRM.csv", "some_bucket")
    dataset = config.dataset(dataset_file)

    forecast_stub.add_client_error("describe_dataset", "ResourceNotFoundException")
    forecast_stub.add_response("describe_dataset", {"Status": "ACTIVE"})

    dataset.cli = forecast_stub.client

    assert dataset.status == Status.DOES_NOT_EXIST
    assert dataset.status == "ACTIVE"


@mock_sts
def test_dataset_imports(configuration_data, forecast_stub):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTRM.csv", "some_bucket")
    dataset = config.dataset(dataset_file)

    forecast_stub.add_response(
        "list_dataset_import_jobs",
        {
            "DatasetImportJobs": [
                {
                    "DatasetImportJobArn": "arn::",
                    "DatasetImportJobName": "middle_job",
                    "LastModificationTime": datetime(2018, 1, 1),
                },
                {
                    "DatasetImportJobArn": "arn::",
                    "DatasetImportJobName": "end_job",
                    "LastModificationTime": datetime(2019, 1, 1),
                },
                {
                    "DatasetImportJobArn": "arn::",
                    "DatasetImportJobName": "early_job",
                    "LastModificationTime": datetime(2017, 1, 1),
                },
            ]
        },
    )

    dataset.cli = forecast_stub.client

    ds_imports = dataset.imports
    assert ds_imports[0].get("DatasetImportJobName") == "end_job"
    assert ds_imports[1].get("DatasetImportJobName") == "middle_job"
    assert ds_imports[2].get("DatasetImportJobName") == "early_job"


@mock_sts
def test_dataset_create_noop_errors(configuration_data, forecast_stub):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTRM.csv", "some_bucket")
    dataset = config.dataset(dataset_file)
    configured_dataset = configuration_data.get("RetailDemandTRM").get("Datasets")[2]

    params = {
        "DatasetType": configured_dataset.get("DatasetType"),
        "DatasetName": "RetailDemandTRM",
        "Domain": configured_dataset.get("Domain"),
        "Schema": configured_dataset.get("Schema"),
        "DataFrequency": configured_dataset.get("DataFrequency"),
    }
    create_params = deepcopy(params)
    create_params["Tags"] = [{"Key": "SolutionId", "Value": "SOL0123"}]

    forecast_stub.add_response(
        "describe_dataset", params,
    )

    forecast_stub.add_response(
        "create_dataset", {"DatasetArn": dataset.arn}, create_params
    )

    forecast_stub.add_response(
        "describe_dataset", params,
    )

    dataset.cli = forecast_stub.client
    dataset.create()

    # clobber the values to trigger some exceptions
    # this is likey caused by a user changing configuration unexpectedly
    dataset._dataset_type = DatasetType.RELATED_TIME_SERIES
    dataset._dataset_domain = DatasetDomain.WORK_FORCE
    dataset._data_frequency = DataFrequency("1min")
    dataset._dataset_schema = {}
    with pytest.raises(ValueError) as excinfo:
        dataset.create()

    assert "dataset type" in str(excinfo.value)
    assert "dataset domain" in str(excinfo.value)
    assert "data frequency" in str(excinfo.value)
    assert "dataset schema" in str(excinfo.value)


@mock_sts
def test_dataset_create(configuration_data, forecast_stub):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTRM.csv", "some_bucket")
    dataset = config.dataset(dataset_file)
    configured_dataset = configuration_data.get("RetailDemandTRM").get("Datasets")[2]

    forecast_stub.add_client_error("describe_dataset", "ResourceNotFoundException")
    forecast_stub.add_response("create_dataset", {"DatasetArn": "arn:"})

    # should not call anything
    dataset.cli = forecast_stub.client
    dataset.create()

    forecast_stub.assert_no_pending_responses()
