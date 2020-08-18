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
import os
from datetime import datetime

import boto3
import pytest
from moto import mock_s3
from moto import mock_sts

from shared.Dataset.dataset_file import DatasetFile
from shared.DatasetGroup.dataset_group import DatasetGroup
from shared.config import Config
from shared.status import Status


@pytest.fixture(params=["data.csv", "data.related.csv", "data.metadata.csv"])
def dataset_file(request):
    bucket_name = "somebucket"
    with mock_s3():
        client = boto3.client("s3", region_name=os.environ.get("AWS_REGION"))
        client.create_bucket(Bucket=bucket_name)
        client.put_object(
            Bucket=bucket_name,
            Key=f"train/{request.param}",
            Body=f"contents={request.param}",
        )

        dsf = DatasetFile(request.param, bucket_name)
        dsf.cli = client
        yield dsf


@pytest.fixture
def mock_forecast_dsg_exists(mocker):
    mock_forecast_cli = mocker.MagicMock()
    mock_forecast_cli.describe_dataset_group.return_value = {
        "DatasetGroupName": "data",
        "DatasetGroupArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/data",
        "DatasetArns": [],
        "Domain": "RETAIL",
        "Status": "ACTIVE",
        "CreationTime": datetime(2015, 1, 1),
        "LastModificationTime": datetime(2015, 1, 1),
    }
    return mock_forecast_cli


@mock_sts
def test_create(dataset_file, configuration_data):
    config = Config()
    config.config = configuration_data

    dataset_group = config.dataset_group(dataset_file)
    dsg = DatasetGroup(
        dataset_group_name=dataset_group.dataset_group_name,
        dataset_domain=dataset_group.dataset_group_domain,
    )

    assert dsg.arn == "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/data"


@mock_sts
def test_status(dataset_file, configuration_data, mock_forecast_dsg_exists):
    config = Config()
    config.config = configuration_data

    dataset_group = config.dataset_group(dataset_file)
    dsg = DatasetGroup(
        dataset_group_name=dataset_group.dataset_group_name,
        dataset_domain=dataset_group.dataset_group_domain,
    )

    dsg.cli = mock_forecast_dsg_exists
    assert dsg.status == Status.ACTIVE
