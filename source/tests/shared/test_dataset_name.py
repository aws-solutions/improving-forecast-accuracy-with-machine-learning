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

from shared.Dataset.dataset_name import DatasetName


def test_dataset_name_valid():
    dsn = DatasetName("some_retail_dataset_name")
    assert dsn == "some_retail_dataset_name"


def test_dataset_name_repr():
    dsn = DatasetName("some_retail_dataset_name")
    assert repr(dsn) == "DatasetName(name='some_retail_dataset_name')"


def test_dataset_name_invalid():
    with pytest.raises(ValueError) as ex:
        DatasetName("Invalid-Name")

    assert (
        str(ex.value)
        == "Dataset name (Invalid-Name) must match ^[a-zA-Z][a-zA-Z0-9_]{0,62}$"
    )


def test_dataset_name_invalid_len():
    name = "a" * 64
    with pytest.raises(ValueError) as ex:
        DatasetName(name)

    assert (
        str(ex.value)
        == f"Dataset name ({name}) is longer than the allowed maximum length (63)"
    )


def test_dataset_name_equality():
    name = "some_test_name"
    dsn = DatasetName(name)
    assert dsn == name
