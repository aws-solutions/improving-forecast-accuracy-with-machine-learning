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

import os

import pytest
from botocore.exceptions import ParamValidationError
from moto import mock_sts

from shared.helpers import (
    step_function_step,
    get_aws_region,
    get_aws_account_id,
    get_forecast_client,
    get_sns_client,
    ResourceFailed,
    ResourceInvalid,
    ResourcePending,
    EnvironmentVariableError,
    get_s3_client,
    get_sfn_client,
    InputValidator,
    get_quicksight_client,
    get_iam_client,
    get_sts_client,
    get_aws_partition,
)
from shared.status import Status


@pytest.fixture
def wrapped_function():
    @step_function_step
    def func_to_wrap(status, context):
        return (status, "arn:")

    return func_to_wrap


@pytest.fixture
def wrapped_function_exception():
    @step_function_step
    def func_to_wrap(exc, context):
        raise exc

    return func_to_wrap


@mock_sts
@pytest.mark.parametrize(
    "exc",
    [
        "Quota limit of n concurrently creating dataset import jobs has been reached",
        "Quota limit of n concurrently creating forecasts has been reached",
        "Quota limit of n concurrently creating forecast exports has been reached",
        "Quota limit of n dataset import jobs has been reached",
    ],
)
def test_concurrent_limit_exceptions(wrapped_function_exception, exc):
    exc = get_forecast_client().exceptions.LimitExceededException(
        {"Error": {"Code": 400, "Message": exc}}, "some_operation"
    )
    with pytest.raises(ResourcePending):
        wrapped_function_exception(exc, None)


@mock_sts
@pytest.mark.parametrize(
    "exc",
    [
        "Quota limit of n of total number of forecast exports has been reached",
        "Quota limit of n GB data size has been reached",
    ],
)
def test_not_concurrent_limit_exceptions(wrapped_function_exception, exc):
    exc = get_forecast_client().exceptions.LimitExceededException(
        {"Error": {"Code": 600, "Message": exc}}, "some_operation"
    )
    with pytest.raises(type(exc)):
        wrapped_function_exception(exc, None)


def test_step_function_step_failed(wrapped_function):
    for status in [Status.UPDATE_FAILED, Status.DELETE_FAILED, Status.CREATE_FAILED]:
        with pytest.raises(ResourceFailed):
            wrapped_function(status, None)


def test_step_function_step_pending(wrapped_function):
    for status in [Status.UPDATE_PENDING, Status.UPDATE_PENDING, Status.CREATE_PENDING]:
        with pytest.raises(ResourcePending):
            wrapped_function(status, None)


def test_step_function_step_finalized(wrapped_function):
    arn = wrapped_function(Status.ACTIVE, None)
    assert arn == "arn:"


def test_step_function_step_invalid(wrapped_function):
    with pytest.raises(ResourceInvalid):
        wrapped_function(Status.DOES_NOT_EXIST, None)


def test_region_missing():
    region = os.environ.pop("AWS_REGION")
    with pytest.raises(EnvironmentVariableError):
        get_aws_region()
    os.environ["AWS_REGION"] = region


@pytest.mark.parametrize(
    "client,url",
    [
        (get_sns_client, "https://sns.us-east-1.amazonaws.com"),
        (get_forecast_client, "https://forecast.us-east-1.amazonaws.com"),
        (get_s3_client, "https://s3"),
        (get_quicksight_client, "https://quicksight"),
        (get_iam_client, "https://iam"),
        (get_sfn_client, "https://states.us-east-1.amazonaws.com"),
        (get_sts_client, "https://sts.amazonaws.com"),
    ],
    ids="sns,forecast,s3,quicksight,iam,sfn,sts".split(","),
)
def test_client_getters(client, url, monkeypatch):
    cli = client()
    assert url in cli.meta.endpoint_url


@mock_sts
def test_with_account_id():
    assert get_aws_account_id() == "abcdefghijkl"


def test_input_validator_invalid():
    iv = InputValidator("create_dataset_group")

    with pytest.raises(ParamValidationError):
        iv.validate()


def test_input_validator_valid():
    # there should be no error in the following call
    iv = InputValidator(
        "create_dataset_group", Domain="RETAIL", DatasetGroupName="Testing123"
    )


def test_aws_partition():
    region = os.environ.pop("AWS_REGION")
    with pytest.raises(EnvironmentVariableError):
        get_aws_region()
    os.environ["AWS_REGION"] = region


def test_cn_partition(monkeypatch):
    """Set the SECRET env var to assert the behavior."""
    monkeypatch.setenv("AWS_REGION", "cn-north-1")
    assert get_aws_region() == "cn-north-1"
    assert get_aws_partition() == "aws-cn"


def test_us_gov_cloud_partition(monkeypatch):
    """Set the SECRET env var to assert the behavior."""
    monkeypatch.setenv("AWS_REGION", "us-gov-east-1")
    assert get_aws_region() == "us-gov-east-1"
    assert get_aws_partition() == "aws-us-gov"
