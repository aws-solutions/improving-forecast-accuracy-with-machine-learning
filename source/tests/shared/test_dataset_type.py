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

import pytest

from shared.Dataset.dataset_type import DatasetType


@pytest.fixture
def valid_dataset_types():
    return ["TARGET_TIME_SERIES", "RELATED_TIME_SERIES", "ITEM_METADATA"]


def test_dataset_type_valid(valid_dataset_types):
    for typ in valid_dataset_types:
        enum = DatasetType[typ]
        assert enum == typ


def test_dataset_type_invalid():
    with pytest.raises(KeyError):
        DatasetType["NOT_REAL"]


def test_dataset_domain_equality():
    typ = DatasetType["TARGET_TIME_SERIES"]
    assert typ == "TARGET_TIME_SERIES"
    assert DatasetType.TARGET_TIME_SERIES == typ


def test_str(valid_dataset_types):
    for typ in valid_dataset_types:
        assert str(DatasetType[typ]) == typ


@pytest.mark.parametrize(
    "dataset_type_str,suffix",
    [
        ("TARGET_TIME_SERIES", ".csv"),
        ("RELATED_TIME_SERIES", ".related.csv"),
        ("ITEM_METADATA", ".metadata.csv"),
    ],
)
def test_suffix(dataset_type_str, suffix):
    typ = DatasetType[dataset_type_str]
    assert typ.suffix == suffix
