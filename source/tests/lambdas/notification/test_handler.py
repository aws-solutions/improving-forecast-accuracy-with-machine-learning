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
import os
from unittest.mock import patch

import pytest

import lambdas.notification.handler as handler
from shared.config import ConfigNotFound, Config


@pytest.fixture
def s3_event():
    return {
        "Records": [
            {
                "eventVersion": "2.2",
                "s3": {
                    "bucket": {
                        "name": "test-bucket",
                    },
                    "object": {"key": "test-key.csv"},
                },
            }
        ]
    }


@patch("lambdas.notification.handler.Config")
def test_notification(mock_config, s3_event, mocker):
    # an invalid configuration - default without any of the required keys
    config = Config()
    config.config = {"my": {"name": "bob"}}

    # simulate invalid config
    config_mock = mocker.MagicMock()
    config_mock.from_s3.return_value = config
    mocker.patch("lambdas.notification.handler.Config", config_mock)

    mock_config = mock_config.return_value
    mock_config.from_s3.return_value = Config()

    client_mock = mocker.MagicMock()
    mocker.patch("lambdas.notification.handler.get_sfn_client", client_mock)

    handler.notification(s3_event, None)

    args, kwargs = client_mock().start_execution.call_args
    assert kwargs.get("stateMachineArn") == os.getenv("STATE_MACHINE_ARN")
    assert "test-key_target_time_series_" in kwargs.get("name")
    assert "input" in kwargs.keys()


def test_config_not_found(s3_event, mocker):
    client_mock = mocker.MagicMock()
    mocker.patch("lambdas.notification.handler.get_sfn_client", client_mock)

    # simulate config not found
    config_mock = mocker.MagicMock()
    config_mock.from_s3.side_effect = ConfigNotFound
    mocker.patch("lambdas.notification.handler.Config", config_mock)

    handler.notification(s3_event, None)

    # ensure it was handled correctly
    args, kwargs = client_mock().start_execution.call_args
    assert (
        json.loads(kwargs.get("input")).get("serviceError").get("Error")
        == "ConfigNotFound"
    )


def test_config_problem(s3_event, mocker):
    client_mock = mocker.MagicMock()
    mocker.patch("lambdas.notification.handler.get_sfn_client", client_mock)

    # simulate config not found
    config_mock = mocker.MagicMock()
    config_mock.from_s3.side_effect = ValueError
    mocker.patch("lambdas.notification.handler.Config", config_mock)

    handler.notification(s3_event, None)

    # ensure it was handled correctly
    args, kwargs = client_mock().start_execution.call_args
    assert (
        json.loads(kwargs.get("input")).get("serviceError").get("Error") == "ValueError"
    )


def test_config_file_problem(s3_event, mocker, caplog):
    config = Config()

    # an invalid configuration - default without any of the required keys
    config.config = {"Default": {}}

    # simulate invalid config
    config_mock = mocker.MagicMock()
    config_mock.from_s3.return_value = config
    mocker.patch("lambdas.notification.handler.Config", config_mock)

    client_mock = mocker.MagicMock()
    mocker.patch("lambdas.notification.handler.get_sfn_client", client_mock)

    handler.notification(s3_event, None)

    args, kwargs = client_mock().start_execution.call_args
    assert (
        json.loads(kwargs.get("input")).get("serviceError").get("Error")
        == "ConfigError"
    )
