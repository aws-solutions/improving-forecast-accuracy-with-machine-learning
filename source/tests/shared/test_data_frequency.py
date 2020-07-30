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

from shared.Dataset.data_frequency import DataFrequency


@pytest.fixture
def valid_frequencies():
    return "Y|M|W|D|H|30min|15min|10min|5min|1min".split("|")


def test_dataset_frequency_valid(valid_frequencies):
    for valid_frequency in valid_frequencies:
        frequency = DataFrequency(valid_frequency)
        assert frequency == valid_frequency


def test_dataset_frequency_invalid():
    with pytest.raises(ValueError) as ex:
        DataFrequency("1y")

    assert (
        str(ex.value)
        == "Invalid frequency. Frequency 1y does not match ^Y|M|W|D|H|30min|15min|10min|5min|1min$"
    )


def test_dataset_frequency_repr(valid_frequencies):
    for frequency in valid_frequencies:
        freq = DataFrequency(frequency=frequency)
        assert repr(freq) == f"DataFrequency(frequency='{frequency}')"


def test_dataset_frequency_str(valid_frequencies):
    for frequency in valid_frequencies:
        freq = DataFrequency(frequency=frequency)
        assert str(freq) == frequency
