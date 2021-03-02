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

import json

import boto3
import pytest
from moto import mock_sns

from lambdas.sns.handler import (
    sns,
    prepare_forecast_ready_message,
)

fail_state_error_message = json.loads(
    """
{
  "bucket": "some_bucket",
  "dataset_file": "train/some_forecast_name.csv",
  "DatasetGroupArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/some_forecast_name",
  "DatasetArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset/some_forecast_name",
  "statesError": {
    "Error": "ValueError",
    "Cause": "{\\"errorMessage\\": \\"configuration item missing key or value for Dataset.TimestampFormat\\", \\"errorType\\": \\"ValueError\\", \\"stackTrace\\": [\\"  File \\\\\\"/var/task/shared/helpers.py\\\\\\", line 25, in wrapper\\\\n    (status, output) = f(event, context)\\\\n\\", \\"  File \\\\\\"/var/task/handler.py\\\\\\", line 12, in createdatasetimportjob\\\\n    dataset_import = config.dataset_import_job(dataset_file)\\\\n\\", \\"  File \\\\\\"/var/task/shared/config.py\\\\\\", line 117, in dataset_import_job\\\\n    timestamp_format=self.data_timestamp_format(dataset_file))\\\\n\\", \\"  File \\\\\\"/var/task/shared/config.py\\\\\\", line 67, in data_timestamp_format\\\\n    format = self.config_item(dataset_file, 'Dataset.TimestampFormat')\\\\n\\", \\"  File \\\\\\"/var/task/shared/config.py\\\\\\", line 50, in config_item\\\\n    raise ValueError(f\\\\\\"configuration item missing key or value for {item}\\\\\\")\\\\n\\"]}"
  }
}
"""
)

fail_service_error_message = json.loads(
    """
{
  "bucket": "some_bucket",
  "dataset_file": "train/some_forecast_name.csv",
  "DatasetGroupArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/some_forecast_name",
  "DatasetArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset/some_forecast_name",
  "serviceError": {
    "Error": "ValueError",
    "Cause": "{\\"errorMessage\\": \\"configuration item missing key or value for Dataset.TimestampFormat\\", \\"errorType\\": \\"ValueError\\", \\"stackTrace\\": [\\"  File \\\\\\"/var/task/shared/helpers.py\\\\\\", line 25, in wrapper\\\\n    (status, output) = f(event, context)\\\\n\\", \\"  File \\\\\\"/var/task/handler.py\\\\\\", line 12, in createdatasetimportjob\\\\n    dataset_import = config.dataset_import_job(dataset_file)\\\\n\\", \\"  File \\\\\\"/var/task/shared/config.py\\\\\\", line 117, in dataset_import_job\\\\n    timestamp_format=self.data_timestamp_format(dataset_file))\\\\n\\", \\"  File \\\\\\"/var/task/shared/config.py\\\\\\", line 67, in data_timestamp_format\\\\n    format = self.config_item(dataset_file, 'Dataset.TimestampFormat')\\\\n\\", \\"  File \\\\\\"/var/task/shared/config.py\\\\\\", line 50, in config_item\\\\n    raise ValueError(f\\\\\\"configuration item missing key or value for {item}\\\\\\")\\\\n\\"]}"
  }
}
"""
)

in_progress_message = json.loads(
    """
{
  "bucket": "some_bucket",
  "dataset_file": "train/some_forecast_name.csv",
  "DatasetGroupArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/some_forecast_name",
  "DatasetArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset/some_forecast_name",
  "serviceError": {
    "Error": "DatasetsImporting",
    "Cause": "{\\"errorMessage\\": \\"in progress message goes here\\"}"
  }
}
"""
)

succeed = json.loads(
    """
{
  "bucket": "some_bucket",
  "dataset_file": "train/some_forecast_name.csv",
  "dataset_group_name": "some_forecast_from_some_forecast_name"
}
"""
)

succeed_related = json.loads(
    """
{
  "bucket": "some_bucket",
  "dataset_file": "train/some_forecast_name.related.csv",
  "dataset_group_name": "some_forecast_from_some_forecast_name"
}
"""
)


@pytest.fixture
def fail_state_error():
    return fail_state_error_message


@pytest.fixture
def fail_service_error():
    return fail_service_error_message


@pytest.fixture
def in_progress():
    return in_progress_message


@pytest.fixture
def success_event():
    return succeed


@pytest.fixture
def success_event_related():
    return succeed_related


@pytest.fixture(scope="function")
def mocked_sns():
    with mock_sns() as mocked_sns:
        cli = boto3.client("sns", region_name="us-east-1")
        cli.create_topic(Name="some-forecast-notification-topic")
        yield cli


def test_sns_notification(fail_state_error, mocked_sns):
    sns(fail_state_error, None)


def test_sns_notification_success(success_event, mocked_sns):
    sns(success_event, None)


def test_sns_notification_in_progress(in_progress, mocker):
    patched_client = mocker.patch("lambdas.sns.handler.get_sns_client")
    sns(in_progress, None)
    patched_client().publish.assert_called_once()
    args, kwargs = patched_client().publish.call_args_list[0]
    assert args == ()
    assert "Update for forecast" in kwargs["Message"]


def test_prepare_forecast_ready_message(success_event):
    dataset_group_name = "some_forecast_from_some_forecast_name"
    assert (
        prepare_forecast_ready_message(success_event)
        == f"Forecast for some_forecast_from_some_forecast_name is ready!"
    )
