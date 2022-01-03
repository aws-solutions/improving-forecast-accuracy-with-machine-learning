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
from typing import Optional

from aws_cdk.core import (
    Construct,
    CfnResource,
    Aws,
    Stack,
)

from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression


class ResourceName(Construct):
    """Used to create unique resource names of the format {stack_name}-{purpose}-{id}"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        purpose: str,
        max_length: int,
        resource_id: Optional[str] = None,
    ):
        super().__init__(scope, construct_id)

        uuid = "ResourceNameFunction-d45b185a-fe34-44ab-a375-17f89597d9ec"
        stack = Stack.of(self)
        self._resource_name_function = stack.node.try_find_child(uuid)

        if not self._resource_name_function:
            self._resource_name_function = SolutionsPythonFunction(
                stack,
                uuid,
                entrypoint=Path(__file__).parent
                / "src"
                / "custom_resources"
                / "name.py",
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
            "StackName": Aws.STACK_NAME,
            "MaxLength": max_length,
        }
        if resource_id:
            properties["Id"] = resource_id

        self.logical_name = f"{construct_id}NameResource"
        self.resource_name_resource = CfnResource(
            self,
            self.logical_name,
            type="Custom::ResourceName",
            properties=properties,
        )

    @property
    def resource_name(self):
        return self.resource_name_resource.get_att("Name")

    @property
    def resource_id(self):
        return self.resource_name_resource.get_att("Id")
