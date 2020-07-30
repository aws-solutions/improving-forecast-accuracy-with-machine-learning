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

import pytest
from botocore.exceptions import ParamValidationError
from moto import mock_sts

from shared.helpers import (
    step_function_step,
    get_aws_region,
    get_account_id,
    get_forecast_client,
    get_sns_client,
    ResourceFailed,
    ResourceInvalid,
    ResourcePending,
    EnvironmentVariableError,
    get_s3_client,
    get_sfn_client,
    InputValidator,
)
from shared.status import Status


@pytest.fixture
def wrapped_function():
    @step_function_step
    def func_to_wrap(status, context):
        return (status, "arn:")

    return func_to_wrap


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


@mock_sts
def test_with_account_id():
    assert get_account_id() == "abcdefghijkl"


def test_forecast_getter():
    cli = get_sns_client()
    assert "https://sns.us-east-1.amazonaws.com" in cli.meta.endpoint_url


def test_sns_getter():
    cli = get_forecast_client()
    assert "https://forecast.us-east-1.amazonaws.com" in cli.meta.endpoint_url


def test_s3_getter():
    cli = get_s3_client()
    assert "https://s3" in cli.meta.endpoint_url


def test_sfn_getter():
    cli = get_sfn_client()
    assert "https://states.us-east-1.amazonaws.com" in cli.meta.endpoint_url


def test_input_validator_invalid():
    iv = InputValidator("create_dataset_group")

    with pytest.raises(ParamValidationError):
        iv.validate()


def test_input_validator_valid():
    # there should be no error in the following call
    iv = InputValidator(
        "create_dataset_group", Domain="RETAIL", DatasetGroupName="Testing123"
    )
