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

from pathlib import Path

from aws_cdk.core import Construct, Stack

from aws_solutions.cdk.aws_lambda.python.layer import SolutionsPythonLayerVersion


class PowertoolsLayer(SolutionsPythonLayerVersion):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        requirements_path: Path = Path(__file__).absolute().parent / "requirements"
        super().__init__(scope, construct_id, requirements_path, **kwargs)

    @staticmethod
    def get_or_create(scope: Construct, **kwargs):
        stack = Stack.of(scope)
        construct_id = "PowertoolsLayer-8E932F0F-197D-4026-A354-23D184C2A624"
        exists = stack.node.try_find_child(construct_id)
        if exists:
            return exists
        return PowertoolsLayer(stack, construct_id, **kwargs)
