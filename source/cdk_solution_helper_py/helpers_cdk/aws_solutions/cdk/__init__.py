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

from aws_solutions.cdk.context import SolutionContext
from aws_solutions.cdk.stack import SolutionStack
from aws_solutions.cdk.synthesizers import SolutionStackSubstitions


class CDKSolution:
    """
    A CDKSolution stores helper utilities for building AWS Solutions using the AWS CDK in Python

    :type cdk_json_path: Path
    :param cdk_json_path: The full path to the cdk.json context for your application
    :type qualifier: str
    :param qualifier: A string that is added to all resources in the CDK bootstrap stack. The default value has no significance.
    """

    def __init__(self, cdk_json_path: Path, qualifier="hnb659fds"):
        self.qualifier = qualifier
        self.context = SolutionContext(cdk_json_path=cdk_json_path)
        self.synthesizer = SolutionStackSubstitions(qualifier=self.qualifier)

    def reset(self) -> None:
        """
        Get a new synthesizer for this CDKSolution - useful for testing
        :return: None
        """
        self.synthesizer = SolutionStackSubstitions(qualifier=self.qualifier)
