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
        == "Invalid frequency. Frequency 1y does not match ^(?:Y|M|W|D|H|30min|15min|10min|5min|1min)$"
    )


def test_dataset_frequency_repr(valid_frequencies):
    for frequency in valid_frequencies:
        freq = DataFrequency(frequency=frequency)
        assert repr(freq) == f"DataFrequency(frequency='{frequency}')"


def test_dataset_frequency_str(valid_frequencies):
    for frequency in valid_frequencies:
        freq = DataFrequency(frequency=frequency)
        assert str(freq) == frequency


def test_data_frequency_total_ordering(valid_frequencies):
    assert len(valid_frequencies) == 10

    for idx_1, f1 in enumerate(valid_frequencies):
        for idx_2, f2 in enumerate(valid_frequencies):
            freq1 = DataFrequency(frequency=f1)
            freq2 = DataFrequency(frequency=f2)

            if idx_1 > idx_2:
                assert freq1 > freq2

            if idx_1 < idx_2:
                assert freq1 < freq2

            if idx_1 == idx_2:
                assert freq1 == freq2

            if idx_1 != idx_2:
                assert freq1 != freq2

            if idx_1 <= idx_2:
                assert freq1 <= freq2

            if idx_1 >= idx_2:
                assert freq1 >= freq2


def test_invalid_data_frequency_comparison():
    frequency = DataFrequency("1min")
    invalid = "not_valid"

    with pytest.raises(ValueError):
        frequency > invalid
