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

import re


class DatasetName:
    """Used to validate dataset names provided in configuration files"""

    valid_dataset_max_len = 63
    valid_dataset_name = re.compile(r"^[a-zA-Z][a-zA-Z0-9_]{0,62}$")

    def __init__(self, name: str):
        self.name = name

    @property
    def name(self) -> str:
        """The dataset name"""
        return self._name

    @name.setter
    def name(self, value: str):
        """Set the dataset name"""
        if len(value) > self.valid_dataset_max_len:
            raise ValueError(
                f"Dataset name ({value}) is longer than the allowed maximum length ({self.valid_dataset_max_len})"
            )

        if not self.valid_dataset_name.match(value):
            raise ValueError(
                f"Dataset name ({value}) must match {self.valid_dataset_name.pattern}"
            )

        self._name = value

    def __str__(self) -> str:
        return self._name

    def __eq__(self, other):
        return self.name == other

    def __hash__(self):
        return hash(self.name)

    def __repr__(self):
        return f"DatasetName(name='{self.name}')"
