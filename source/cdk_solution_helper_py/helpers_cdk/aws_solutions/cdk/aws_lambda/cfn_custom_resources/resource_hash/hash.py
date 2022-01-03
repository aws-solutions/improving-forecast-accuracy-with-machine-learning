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

from aws_cdk.core import (
    Construct,
    CfnResource,
    Stack,
)

from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression


class ResourceHash(Construct):
    """Used to create unique resource names based on the hash of the stack ID"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        purpose: str,
        max_length: int,
    ):
        super().__init__(scope, construct_id)

        uuid = "ResourceHashFunction-b8785f53-1531-4bfb-a119-26aa638d7b19"
        stack = Stack.of(self)
        self._resource_name_function = stack.node.try_find_child(uuid)

        if not self._resource_name_function:
            self._resource_name_function = SolutionsPythonFunction(
                stack,
                uuid,
                entrypoint=Path(__file__).parent
                / "src"
                / "custom_resources"
                / "hash.py",
                function="handler",
            )
            add_cfn_nag_suppressions(
                resource=self._resource_name_function.node.default_child,
                suppressions=[
                    CfnNagSuppression(
                        "W89", "This AWS Lambda Function is not deployed to a VPC"
                    ),
                    CfnNagSuppression(
                        "W92",
                        "This AWS Lambda Function does not require reserved concurrency",
                    ),
                ],
            )

        properties = {
            "ServiceToken": self._resource_name_function.function_arn,
            "Purpose": purpose,
            "MaxLength": max_length,
        }

        self.logical_name = f"{construct_id}HashResource"
        self.resource_name_resource = CfnResource(
            self,
            self.logical_name,
            type="Custom::ResourceHash",
            properties=properties,
        )

    @property
    def resource_name(self):
        return self.resource_name_resource.get_att("Name")

    @property
    def resource_id(self):
        return self.resource_name_resource.get_att("Id")
