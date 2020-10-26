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

from shared.Dataset.dataset_domain import DatasetDomain
from shared.Dataset.dataset_file import DatasetFile
from shared.DatasetGroup.dataset_group import DatasetGroup
from shared.DatasetGroup.dataset_group_name import DatasetGroupName
from shared.config import Config
from shared.helpers import DatasetsImporting
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


@mock_sts
@pytest.mark.parametrize(
    "domain,identifier,metric,fields",
    [
        ("RETAIL", "item_id", "demand", ["item_id", "timestamp", "demand"]),
        ("CUSTOM", "item_id", "target_value", ["item_id", "timestamp", "target_value"]),
        ("INVENTORY_PLANNING", "item_id", "demand", ["item_id", "timestamp", "demand"]),
        (
            "EC2_CAPACITY",
            "instance_type",
            "number_of_instances",
            ["instance_type", "timestamp", "number_of_instances"],
        ),
        (
            "WORK_FORCE",
            "workforce_type",
            "workforce_demand",
            ["workforce_type", "timestamp", "workforce_demand"],
        ),
        (
            "METRICS",
            "metric_name",
            "metric_value",
            ["metric_name", "timestamp", "metric_value"],
        ),
    ],
    ids="RETAIL,CUSTOM,INVENTORY_PLANNING,EC2_CAPACITY,WORK_FORCE,METRICS".split(","),
)
def test_schema(domain, identifier, metric, fields):
    dsg = DatasetGroup(
        dataset_domain=DatasetDomain[domain],
        dataset_group_name=DatasetGroupName("some_name"),
    )

    assert dsg.schema.dataset_group == dsg
    assert dsg.schema.identifier == identifier
    assert dsg.schema.metric == metric
    for field in dsg.schema.fields:
        assert field in fields


@pytest.fixture
def mocked_dsg(dataset_file, configuration_data, mocker):
    with mock_sts():
        config = Config()
        config.config = configuration_data

        dataset_group = config.dataset_group(dataset_file)
        dsg = DatasetGroup(
            dataset_group_name=dataset_group.dataset_group_name,
            dataset_domain=dataset_group.dataset_group_domain,
        )

        dsg.cli = mocker.MagicMock()
        dsg.cli.describe_dataset_group.return_value = {
            "DatasetArns": ["arn::1", "arn::2", "arn::3"]
        }
        dsg.cli.describe_dataset.return_value = {"DatasetArn": 'arn::1', "Status": "ACTIVE", 'LastModificationTime': datetime.now()}

        dsg.cli.get_paginator().paginate.return_value = [{
            'DatasetImportJobs': [
                {
                    "DatasetImportJobArn": f"arn::{i}",
                    "Status": "ACTIVE",
                    "LastModificationTime": datetime.now()
                }
                for i in range(3)
            ]
        }]

        yield dsg


def test_dataset_list(mocked_dsg):
    datasets = mocked_dsg.datasets

    assert len(datasets) == 3
    assert all({"some": "info"} for dataset in datasets)


def test_dataset_ready(mocked_dsg):
    assert mocked_dsg.ready()


def test_dataset_not_ready(mocked_dsg):
    mocked_dsg.cli.describe_dataset.return_value = {"Status": "CREATE_IN_PROGRESS"}
    with pytest.raises(DatasetsImporting):
        assert not mocked_dsg.ready()


def test_latest_timestamp(mocked_dsg):
    dates = [datetime(2002, 1, 1), datetime(2000, 1, 1), datetime(2001, 1, 1)]

    def side_effect(DatasetArn):
        return {"LastModificationTime": dates.pop()}

    mocked_dsg.cli.describe_dataset.side_effect = side_effect
    result = mocked_dsg.latest_timestamp
    assert result == "2002_01_01_00_00_00"
