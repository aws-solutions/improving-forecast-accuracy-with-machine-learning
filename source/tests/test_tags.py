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

from shared.Tags.tags import (
    validate_tags,
    _get_present,
    _get_absent,
    _tags_as_list,
    _tags_as_dict,
    DISALLOW_TAGS,
    get_tags,
    get_untags,
)

VALID_TAGS = [
    {"Key": "a", "Value": "b", "State": "Present"},
    {"Key": "b", "Value": "b", "State": "Absent"},
    {"Key": "c", "State": "Absent"},
    {"Key": "d", "Value": "", "State": "Present"},
    {"Key": "e"},
]


@pytest.fixture
def valid_tags():
    return VALID_TAGS


@pytest.mark.parametrize(
    "invalid_tag",
    [
        "a",
        1,
        1.1,
        {"Key": "name", "Value": "value"},
        [1, 2, 3],
        [{}],
        [{"Key": "a", "Value": 1}],
        [{"Key": "a", "Value": {}}],
        [{"key": "a"}],
        [{"Key": "a", "Value": "", "State": "invalid"}],
        [{"Key": "name", "Value": "value"}, {"Key": "name", "Value": "value2"}],
        [{"Key": tag} for tag in DISALLOW_TAGS],
    ],
)
def test_validate_invalid_tags(invalid_tag):
    with pytest.raises(ValueError):
        validate_tags(invalid_tag)


def test_validate_valid_tags(valid_tags):
    validate_tags(valid_tags)


def test_get_present(valid_tags):
    assert _tags_as_dict(_get_present(valid_tags)) == {"a": "b", "d": "", "e": ""}


def test_get_absent(valid_tags):
    assert _get_absent(valid_tags) == ["b", "c"]


def test_get_tags_as_list():
    assert (
        _tags_as_list(
            {
                "key1": "value1",
                "key2": "value2",
            }
        )
        == [{"Key": "key1", "Value": "value1"}, {"Key": "key2", "Value": "value2"}]
    )


def test_get_tags():
    resource_tags = [
        {"Key": "owner", "Value": "owner_resource"},
        {"Key": "removed_resource", "State": "Absent"},
        {"Key": "added_resource", "Value": "resource_add"},
        {"Key": "already_gone_resource", "State": "Absent"},
    ]
    global_tags = [
        {"Key": "owner", "Value": "owner_global"},
        {"Key": "removed_globally", "State": "Absent"},
        {"Key": "added_globally", "Value": "global_add"},
        {"Key": "already_gone_global", "State": "Absent"},
    ]
    active_tags = _tags_as_list(
        {
            "owner": "owner_active",
            "LatestDatasetUpdateETag": "abcdef",
            "LatestDatasetUpdateName": "demand_15.metadata.csv",
            "SolutionId": "SO0123Test",
            "removed_globally": "to-remove",
            "removed_resource": "to-remove",
        }
    )

    assert get_tags(resource_tags, global_tags, active_tags) == _tags_as_list(
        {
            "owner": "owner_resource",
            "added_globally": "global_add",
            "added_resource": "resource_add",
        }
    )
    assert set(get_untags(resource_tags, global_tags, active_tags)) == {
        "removed_globally",
        "removed_resource",
    }
