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

import re
from functools import total_ordering

_data_frequencies = ["Y", "M", "W", "D", "H", "30min", "15min", "10min", "5min", "1min"]


@total_ordering
class DataFrequency:
    """Used to validate data frequencies provided in configuration files."""

    valid_frequency = re.compile(r"^Y|M|W|D|H|30min|15min|10min|5min|1min$")

    def __init__(self, frequency):
        if not self.valid_frequency.match(frequency):
            raise ValueError(
                f"Invalid frequency. Frequency {frequency} does not match {self.valid_frequency.pattern}"
            )
        self.frequency = frequency

    def __lt__(self, other):
        try:
            idx_self = _data_frequencies.index(str(self))
            idx_other = _data_frequencies.index(str(other))
        except ValueError:
            raise ValueError(
                f"Invalid frequency. Frequency {other} does not match {self.valid_frequency.pattern}"
            )
        return idx_self < idx_other

    def __eq__(self, other):
        return self.frequency == other

    def __str__(self) -> str:
        return self.frequency

    def __repr__(self) -> str:
        return f"DataFrequency(frequency='{self.frequency}')"
