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

from shared.Dataset.data_timestamp_format import DataTimestampFormat


@pytest.fixture
def valid_timestamp_formats():
    return "yyyy-MM-dd|yyyy-MM-dd HH:mm:ss".split("|")


def test_dataset_timestamp_format_valid(valid_timestamp_formats):
    for valid_timestamp_format in valid_timestamp_formats:
        timestamp_format = DataTimestampFormat(format=valid_timestamp_format)
        assert timestamp_format == valid_timestamp_format


def test_dataset_timestamp_format_invalid():
    with pytest.raises(ValueError) as ex:
        DataTimestampFormat("yyy-mm-d")

    assert "Invalid timestamp format." in str(ex.value)


def test_dataset_timestamp_format_repr(valid_timestamp_formats):
    for timestamp_format in valid_timestamp_formats:
        freq = DataTimestampFormat(format=timestamp_format)
        assert repr(freq) == f"DataTimestampFormat(format='{timestamp_format}')"


def test_dataset_timestamp_format_str(valid_timestamp_formats):
    for timestamp_format in valid_timestamp_formats:
        format = DataTimestampFormat(format=timestamp_format)
        assert str(format) == timestamp_format
