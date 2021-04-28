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


class DatasetGroupName:
    """Used to validate dataset names provided in configuration files"""

    valid_dataset_max_len = 63
    valid_dataset_name = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,62}$")

    def __init__(self, name: str):
        self.name = name

    @property
    def name(self) -> str:
        """The dataset group name"""
        return self._name

    @name.setter
    def name(self, value: str):
        """Set the dataset group name"""
        if len(value) > self.valid_dataset_max_len:
            raise ValueError(
                f"Dataset Group name ({value}) is longer than the allowed maximum length ({self.valid_dataset_max_len})"
            )

        if not self.valid_dataset_name.match(value):
            raise ValueError(
                f"Dataset Group name ({value}) must match {self.valid_dataset_name.pattern}"
            )

        self._name = value

    def __str__(self) -> str:
        return self._name

    def __eq__(self, other):
        return self.name == other

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return f"DatasetGroupName(name='{self.name}')"
