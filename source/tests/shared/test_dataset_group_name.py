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

from shared.DatasetGroup.dataset_group_name import DatasetGroupName


def test_dataset_group_name_valid():
    dsgn = DatasetGroupName("some_retail_dataset_group_name")
    assert dsgn == "some_retail_dataset_group_name"


def test_dataset_group_name_repr():
    dsgn = DatasetGroupName("some_retail_dataset_group_name")
    assert repr(dsgn) == "DatasetGroupName(name='some_retail_dataset_group_name')"


def test_dataset_group_name_invalid():
    with pytest.raises(ValueError) as ex:
        DatasetGroupName("Invalid-Name")

    assert (
        str(ex.value)
        == "Dataset Group name (Invalid-Name) must match ^[a-zA-Z]\\w{0,62}$"
    )


def test_dataset_group_name_invalid_len():
    name = "a" * 64
    with pytest.raises(ValueError) as ex:
        DatasetGroupName(name)

    assert (
        str(ex.value)
        == f"Dataset Group name ({name}) is longer than the allowed maximum length (63)"
    )


def test_dataset_group_name_equality():
    name = "some_test_name"
    dsn = DatasetGroupName(name)
    assert dsn == name


def test_dataset_group_name_compare():
    dsn1 = DatasetGroupName("test")
    dsn2 = DatasetGroupName("test")

    assert dsn1 == dsn2
