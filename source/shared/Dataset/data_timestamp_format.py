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


class DataTimestampFormat:
    """Used to validate data timestamp formats provided in configuration files."""

    valid_format = re.compile(r"^yyyy-MM-dd|yyyy-MM-dd HH:mm:ss$")

    def __init__(self, format):
        if not self.valid_format.match(format):
            raise ValueError(
                f"Invalid timestamp format. Format {format} does not match {self.valid_format.pattern}"
            )
        self.format = format

    def __str__(self) -> str:
        return self.format

    def __repr__(self) -> str:
        return f"DataTimestampFormat(format='{self.format}')"

    def __eq__(self, other):
        return self.format == other
