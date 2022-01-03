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

import json
import os
from collections import namedtuple

import boto3
import pytest
from moto import mock_sns, mock_sqs

from lambdas.sns.handler import (
    sns,
    MessageBuilder,
)

fail_state_error_message = json.loads(
    """
{
  "bucket": "some_bucket",
  "dataset_file": "train/some_forecast_name.csv",
  "DatasetGroupArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/some_forecast_name",
  "DatasetArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset/some_forecast_name",
  "error": {
      "statesError": {
        "Error": "ValueError",
        "Cause": "{\\"errorMessage\\": \\"configuration item missing key or value for Dataset.TimestampFormat\\", \\"errorType\\": \\"ValueError\\", \\"stackTrace\\": [\\"  File \\\\\\"/var/task/shared/helpers.py\\\\\\", line 25, in wrapper\\\\n    (status, output) = f(event, context)\\\\n\\", \\"  File \\\\\\"/var/task/handler.py\\\\\\", line 12, in createdatasetimportjob\\\\n    dataset_import = config.dataset_import_job(dataset_file)\\\\n\\", \\"  File \\\\\\"/var/task/shared/config.py\\\\\\", line 117, in dataset_import_job\\\\n    timestamp_format=self.data_timestamp_format(dataset_file))\\\\n\\", \\"  File \\\\\\"/var/task/shared/config.py\\\\\\", line 67, in data_timestamp_format\\\\n    format = self.config_item(dataset_file, 'Dataset.TimestampFormat')\\\\n\\", \\"  File \\\\\\"/var/task/shared/config.py\\\\\\", line 50, in config_item\\\\n    raise ValueError(f\\\\\\"configuration item missing key or value for {item}\\\\\\")\\\\n\\"]}"
      }
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
  "error": {
      "serviceError": {
        "Error": "ValueError",
        "Cause": "{\\"errorMessage\\": \\"configuration item missing key or value for Dataset.TimestampFormat\\", \\"errorType\\": \\"ValueError\\", \\"stackTrace\\": [\\"  File \\\\\\"/var/task/shared/helpers.py\\\\\\", line 25, in wrapper\\\\n    (status, output) = f(event, context)\\\\n\\", \\"  File \\\\\\"/var/task/handler.py\\\\\\", line 12, in createdatasetimportjob\\\\n    dataset_import = config.dataset_import_job(dataset_file)\\\\n\\", \\"  File \\\\\\"/var/task/shared/config.py\\\\\\", line 117, in dataset_import_job\\\\n    timestamp_format=self.data_timestamp_format(dataset_file))\\\\n\\", \\"  File \\\\\\"/var/task/shared/config.py\\\\\\", line 67, in data_timestamp_format\\\\n    format = self.config_item(dataset_file, 'Dataset.TimestampFormat')\\\\n\\", \\"  File \\\\\\"/var/task/shared/config.py\\\\\\", line 50, in config_item\\\\n    raise ValueError(f\\\\\\"configuration item missing key or value for {item}\\\\\\")\\\\n\\"]}"
      }
  }
}
"""
)

fail_glue_error_message = json.loads(
    """
{
  "bucket": "some_bucket",
  "dataset_file": "train/some_forecast_name.csv",
  "DatasetGroupArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/some_forecast_name",
  "DatasetArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset/some_forecast_name",
  "error": {
    "Error": "Glue.ConcurrentRunsExceededException",
    "Cause": "Concurrent runs exceeded for stack-name-ETL (Service: AWSGlue; Status Code: 400; Error Code: ConcurrentRunsExceededException; Request ID: random-id; Proxy: null)"
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
  "error": {
      "serviceError": {
        "Error": "DatasetsImporting",
        "Cause": "{\\"errorMessage\\": \\"in progress message goes here\\"}"
      }
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
def glue_error():
    return fail_glue_error_message


@pytest.fixture
def success_event_related():
    return succeed_related


@pytest.fixture
def context():
    ctx = namedtuple("Context", ["invoked_function_arn"])
    return ctx(f"arn:aws:lambda:us-east-1:{'1' * 12}:function:my-function:1")


@pytest.fixture(scope="function")
def mocked_sns():
    topic_arn = os.environ.get("SNS_TOPIC_ARN")
    topic_name = topic_arn.split(":")[-1]

    with mock_sqs():
        with mock_sns():
            cli = boto3.client("sns", region_name="us-east-1")
            cli.create_topic(Name="some-forecast-notification-topic")

            sqs = boto3.client("sqs")
            sqs.create_queue(QueueName="TestQueue")

            cli.subscribe(
                TopicArn=topic_arn,
                Protocol="sqs",
                Endpoint=f"arn:aws:sqs:us-east-1:{'1'*12}:TestQueue",
            )

            yield sqs


def test_sns_notification(fail_state_error, mocked_sns, context):
    sns(fail_state_error, context)


def test_sns_notification_success(success_event, mocked_sns, context):
    sns(success_event, context)


def test_sns_notification_json(fail_state_error, mocked_sns, context):
    dataset_group_name = "some_forecast_name"

    sns(fail_state_error, context)

    url = mocked_sns.get_queue_url(QueueName="TestQueue")["QueueUrl"]
    msg = json.loads(
        json.loads(
            mocked_sns.receive_message(QueueUrl=url, MaxNumberOfMessages=1,)[
                "Messages"
            ][0]["Body"]
        )["Message"]
    )

    error_default = f"Forecast for {dataset_group_name} completed with errors"
    error_json = {
        "datasetGroup": dataset_group_name,
        "status": "UPDATE FAILED",
        "summary": f"Forecast for {dataset_group_name} completed with errors",
        "description": "There was an error running the forecast job for dataset group some_forecast_name\n\nMessage: configuration item missing key or value for Dataset.TimestampFormat",
    }

    assert msg["default"] == error_default
    assert msg["sms"] == error_default
    assert json.loads(msg["sqs"]) == error_json


def test_sns_notification_non_json_cause(glue_error, mocked_sns, context):
    sns(glue_error, context)
    url = mocked_sns.get_queue_url(QueueName="TestQueue")["QueueUrl"]
    msg = json.loads(
        json.loads(
            mocked_sns.receive_message(QueueUrl=url, MaxNumberOfMessages=1,)[
                "Messages"
            ][0]["Body"]
        )["Message"]
    )

    jm = json.loads(msg["sqs"])
    assert jm["status"] == "UPDATE FAILED"
    assert "Concurrent runs" in jm["description"]
