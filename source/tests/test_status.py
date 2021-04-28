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

import pytest

from shared.status import Status


@pytest.fixture
def valid_statuses():
    return "ACTIVE, CREATE_PENDING, CREATE_IN_PROGRESS, CREATE_FAILED, DELETE_PENDING, DELETE_IN_PROGRESS, DELETE_FAILED, UPDATE_PENDING, UPDATE_IN_PROGRESS, UPDATE_FAILED".replace(
        " ", ""
    ).split(
        ","
    )


def test_valid_status_equality(valid_statuses):
    for status in valid_statuses:
        enum = Status[status]
        assert enum == status


def test_status_invalid():
    with pytest.raises(KeyError):
        Status["NOT_REAL"]


def test_status_equality():
    status = Status["ACTIVE"]
    assert status == "ACTIVE"
    assert Status.ACTIVE == status


def test_status_updating():
    statuses_pending = [
        Status.CREATE_PENDING,
        Status.DELETE_PENDING,
        Status.UPDATE_PENDING,
    ]
    for status in statuses_pending:
        assert status.updating


def test_status_failed():
    statuses_failed = [Status.CREATE_FAILED, Status.DELETE_FAILED, Status.UPDATE_FAILED]
    for status in statuses_failed:
        assert status.failed


def test_finalized(valid_statuses):
    for status in valid_statuses:
        if status == "ACTIVE":
            assert Status[status].finalized
        else:
            assert not Status[status].finalized


def test_string_equality(valid_statuses):
    for status in valid_statuses:
        assert status == str(Status[status])
