# ######################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                  #
#                                                                                                                      #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance      #
#  with the License. You may obtain a copy of the License at                                                           #
#                                                                                                                      #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                         #
#                                                                                                                      #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed    #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for   #
#  the specific language governing permissions and limitations under the License.                                      #
# ######################################################################################################################

from dataclasses import dataclass, field

from aws_cdk.aws_lambda import IFunction


@dataclass
class EnvironmentVariable:
    scope: IFunction
    name: str
    value: str = field(default="")

    def __post_init__(self):
        if not self.value:
            self.value = self.scope.node.try_get_context(self.name)
        self.scope.add_environment(self.name, self.value)

    def __str__(self):
        return self.value
