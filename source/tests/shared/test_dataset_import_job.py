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

from datetime import datetime

import boto3
import pytest
from botocore.stub import Stubber
from moto import mock_sts

from shared.Dataset.dataset_file import DatasetFile
from shared.config import Config
from shared.status import Status


@pytest.fixture
def forecast_stub():
    client = boto3.client("forecast", region_name="us-east-1")
    with Stubber(client) as stubber:
        yield stubber


@mock_sts
def test_dataset_import_job_status_lifecycle(configuration_data, forecast_stub, mocker):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTRM.csv", "some_bucket")
    dataset_import_job = config.dataset_import_job(dataset_file)
    size = 40
    etag = "9d2990c88a30dac1785a09fbb46f3e10"

    # first call - doesn't exist
    forecast_stub.add_response("list_dataset_import_jobs", {"DatasetImportJobs": []})
    forecast_stub.add_response(
        "list_dataset_import_jobs",
        {
            "DatasetImportJobs": [
                {
                    "LastModificationTime": datetime(2015, 1, 1),
                    "DatasetImportJobArn": "arn:2015-1-1",
                },
                {
                    "LastModificationTime": datetime(2017, 1, 1),
                    "DatasetImportJobArn": "arn:2017-1-1",
                },
                {
                    "LastModificationTime": datetime(2016, 1, 1),
                    "DatasetImportJobArn": "arn:2016-1-1",
                },
            ]
        },
    )
    forecast_stub.add_response(
        "describe_dataset_import_job",
        {"Status": "ACTIVE"},
    )
    forecast_stub.add_response(
        "list_tags_for_resource", {"Tags": [{"Key": "SolutionETag", "Value": etag}]}
    )
    forecast_stub.add_response("list_tags_for_resource", {"Tags": []})
    forecast_stub.add_response("tag_resource", {})
    forecast_stub.add_response(
        "list_dataset_import_jobs",
        {
            "DatasetImportJobs": [
                {
                    "LastModificationTime": datetime(2015, 1, 1),
                    "DatasetImportJobArn": "arn:2015-1-1",
                },
                {
                    "LastModificationTime": datetime(2017, 1, 1),
                    "DatasetImportJobArn": "arn:2017-1-1",
                },
                {
                    "LastModificationTime": datetime(2016, 1, 1),
                    "DatasetImportJobArn": "arn:2016-1-1",
                },
            ]
        },
    )
    forecast_stub.add_response(
        "describe_dataset_import_job",
        {"Status": "ACTIVE"},
    )
    forecast_stub.add_response(
        "list_tags_for_resource",
        {
            "Tags": [
                {"Key": "SolutionETag", "Value": "9d2990c88a30dac1785a09fbb46f3e11"}
            ]
        },
    )
    forecast_stub.add_response("list_tags_for_resource", {"Tags": []})
    forecast_stub.add_response("tag_resource", {})

    dataset_import_job.cli = forecast_stub.client
    mocker.patch(
        "shared.Dataset.dataset_file.DatasetFile.etag",
        new_callable=mocker.PropertyMock,
        return_value=etag,
    )

    assert dataset_import_job.status == Status.DOES_NOT_EXIST

    # simulate finding an active dataset
    assert dataset_import_job.status == Status.ACTIVE

    # simulate a new dataset (with more lines) uploaded
    assert dataset_import_job.status == Status.DOES_NOT_EXIST


@mock_sts
def test_dataset_import_job_arn(configuration_data, forecast_stub, mocker):
    config = Config()
    config.config = configuration_data

    dataset_file = DatasetFile("RetailDemandTRM.csv", "some_bucket")
    dataset_import_job = config.dataset_import_job(dataset_file)

    # create some job history
    forecast_stub.add_response(
        "list_dataset_import_jobs",
        {
            "DatasetImportJobs": [
                {
                    "LastModificationTime": datetime(2015, 1, 1),
                    "DatasetImportJobArn": "arn:2015-1-1",
                },
                {
                    "LastModificationTime": datetime(2017, 1, 1),
                    "DatasetImportJobArn": "arn:aws:forecast:abcdefghijkl:us-east-1:dataset-import-job/RetailDemandTRM/RetailDemandTRM_2017_01_01_00_00_00",
                },
                {
                    "LastModificationTime": datetime(2016, 1, 1),
                    "DatasetImportJobArn": "arn:2016-1-1",
                },
            ]
        },
    )

    dataset_import_job.cli = forecast_stub.client
    assert (
        dataset_import_job.arn
        == f"arn:aws:forecast:abcdefghijkl:us-east-1:dataset-import-job/RetailDemandTRM/RetailDemandTRM_2017_01_01_00_00_00"
    )
