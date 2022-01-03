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
from pathlib import Path

from aws_cdk.core import Construct

from aws_solutions.cdk.stepfunctions.solutionstep import SolutionStep


class Base(SolutionStep):
    def __init__(
        self,
        scope: Construct,
        id: str,
        name: str,
        handler="handler.py",
        function=None,
        layers=None,
        **kwargs,
    ):
        if not function:
            function = name

        super().__init__(
            scope,
            id,
            layers=layers,
            entrypoint=(
                Path(__file__).absolute().parents[4] / "lambdas" / name / handler
            ),
            function=function,
            **kwargs,
        )

    def _set_permissions(self) -> None:
        pass  # NOSONAR (python:S1186) - no permissions required by default
