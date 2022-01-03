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


class Status(Enum):
    """Represents the status of an Amazon Forecast resource"""

    ACTIVE = auto()
    CREATE_PENDING = auto()
    CREATE_IN_PROGRESS = auto()
    CREATE_FAILED = auto()
    DELETE_PENDING = auto()
    DELETE_IN_PROGRESS = auto()
    DELETE_FAILED = auto()
    UPDATE_PENDING = auto()
    UPDATE_IN_PROGRESS = auto()
    UPDATE_FAILED = auto()
    DOES_NOT_EXIST = auto()
    NOT_READY = auto()

    def __str__(self) -> str:
        return self.name

    def __eq__(self, other):
        return self.name == other

    @property
    def finalized(self):
        """A status is considered finalized if it is ACTIVE"""
        return True if self == "ACTIVE" else False

    @property
    def updating(self):
        """A status is considered to be updating if it is pending or in progress"""
        pending = "PENDING" in self.name
        in_progress = "IN_PROGRESS" in self.name
        return pending or in_progress

    @property
    def failed(self):
        """A status is considered to be failed if its creation, update or deletion has failed"""
        failed = "FAILED" in self.name
        return failed
