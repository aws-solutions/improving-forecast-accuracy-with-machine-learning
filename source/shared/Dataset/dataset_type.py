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

from enum import Enum, auto


class DatasetType(Enum):
    """Used to validate dataset types provided in configuration files"""

    TARGET_TIME_SERIES = auto()
    RELATED_TIME_SERIES = auto()
    ITEM_METADATA = auto()

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return self.name == other

    @property
    def suffix(self):
        if self.name == self.TARGET_TIME_SERIES:
            return ".csv"
        elif self.name == self.RELATED_TIME_SERIES:
            return ".related.csv"
        elif self.name == self.ITEM_METADATA:
            return ".metadata.csv"
        else:
            raise ValueError("Invalid Dataset Type")
