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
from uuid import UUID

import pytest

from lambdas.cloudformation_resources.solution_metrics import (
    helper,
    send_metrics,
    _sanitize_data,
)


@pytest.fixture(params=["Create", "Update", "Delete"])
def test_event(request):
    event = {
        "RequestType": request.param,
        "ResourceProperties": {"Solution": "SOL0123", "Metric1": "Data1"},
    }
    yield event


def test_sanitize_data():
    event = {
        "RequestType": "Create",
        "ResourceProperties": {
            "ServiceToken": "REMOVEME",
            "Resource": "REMOVEME",
            "Solution": "REMOVEME",
            "UUID": "REMOVEME",
            "Keep": "Me",
        },
    }

    result = _sanitize_data(event)
    assert result == {"Keep": "Me", "CFTemplate": "Created"}


def test_send_metrics(test_event):
    test_event["ResourceProperties"]["Resource"] = "UUID"
    send_metrics(test_event, None)

    # raises a ValueError if we didn't get a uuid back
    uuid_obj = UUID(helper.Data["UUID"], version=4)


def test_send_metrics_real(test_event, mocker):
    metrics_endpoint = os.getenv("METRICS_ENDPOINT")
    if metrics_endpoint:
        mocker.patch(
            "lambdas.cloudformation_resources.solution_metrics.METRICS_ENDPOINT",
            metrics_endpoint,
        )
        result = send_metrics(test_event, None)


def test_send_metrics(mocker, test_event):
    requests_mock = mocker.MagicMock()
    mock_endpoint = "https://metrics-endpoint.com/example"
    mocker.patch(
        "lambdas.cloudformation_resources.solution_metrics.requests", requests_mock
    )
    mocker.patch(
        "lambdas.cloudformation_resources.solution_metrics.METRICS_ENDPOINT",
        mock_endpoint,
    )

    result = send_metrics(test_event, None)
    assert UUID(result, version=4)

    assert requests_mock.post.call_args[0][0] == mock_endpoint

    request_data = requests_mock.post.call_args[1].get("json")
    assert request_data.get("Solution") == "SOL0123"
    assert request_data.get("UUID")
    assert request_data.get("TimeStamp")

    data = request_data.get("Data")
    assert data.get("Metric1") == "Data1"
    assert data.get("CFTemplate") in ["Created", "Deleted", "Updated"]

    headers = requests_mock.post.call_args[1].get("headers")
    assert headers.get("Content-Type") == "application/json"


def test_uuid_reuse(mocker, test_event):
    requests_mock = mocker.MagicMock()
    mocker.patch(
        "lambdas.cloudformation_resources.solution_metrics.requests", requests_mock
    )
    uuid_to_set = "b14cc738-4c6c-42eb-b39b-4506a6a76911"

    if test_event.get("RequestType") == "Create":
        # on create, we CloudFormation doesn't send a UUID
        generated_uuid = send_metrics(test_event, None)
        assert UUID(generated_uuid, version=4)
    else:
        # on update/ delete, CloudFormation sends a UUID, and the custom resource should return it as passed
        test_event["PhysicalResourceId"] = uuid_to_set
        generated_uuid = send_metrics(test_event, None)
        assert generated_uuid == uuid_to_set
