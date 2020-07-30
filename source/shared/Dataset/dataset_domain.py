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

from enum import Enum, auto


class DatasetDomain(Enum):
    """Used to validate dataset domains provided in configuration files"""

    RETAIL = auto()
    CUSTOM = auto()
    INVENTORY_PLANNING = auto()
    EC2_CAPACITY = auto()
    WORK_FORCE = auto()
    WEB_TRAFFIC = auto()
    METRICS = auto()

    def __str__(self) -> str:
        return self.name

    def __eq__(self, other):
        return self.name == other
