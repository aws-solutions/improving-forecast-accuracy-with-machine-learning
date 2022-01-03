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

import re
from typing import Optional

from aws_cdk.core import Stack, Construct, NestedStack

from aws_solutions.cdk.aws_lambda.cfn_custom_resources.solutions_metrics import Metrics
from aws_solutions.cdk.interfaces import TemplateOptions
from aws_solutions.cdk.mappings import Mappings

RE_SOLUTION_ID = re.compile(r"^SO\d+(-.*)*$")
RE_TEMPLATE_FILENAME = re.compile(r"^[a-z]+(?:-[a-z]+)*\.template$")  # NOSONAR


def validate_re(name, value, regex: re.Pattern):
    if regex.match(value):
        return value
    raise ValueError(f"{name} must match '{regex.pattern}")


def validate_solution_id(solution_id: str) -> str:
    return validate_re("solution_id", solution_id, RE_SOLUTION_ID)


def validate_template_filename(template_filename: str) -> str:
    return validate_re("template_filename", template_filename, RE_TEMPLATE_FILENAME)


class SolutionStack(Stack):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        description: str,
        template_filename,
        extra_mappings: Optional[dict] = None,
        **kwargs,
    ):
        super().__init__(scope, construct_id, **kwargs)

        self.metrics = {}
        self.solution_id = self.node.try_get_context("SOLUTION_ID")
        self.solution_version = self.node.try_get_context("SOLUTION_VERSION")
        self.mappings = Mappings(
            self, solution_id=self.solution_id, extra_mappings=extra_mappings
        )
        self.solutions_template_filename = validate_template_filename(template_filename)
        self.description = description.strip(".")
        self.solutions_template_options = TemplateOptions(
            self,
            construct_id=construct_id,
            description=f"({self.solution_id}) - {self.description}. Version {self.solution_version}",
            filename=template_filename,
        )

    def _prepare(self) -> None:
        """Called before synthesis, this allows us to set metrics at the end of synthesis"""
        self.metrics = Metrics(self, "Metrics", self.metrics)


class NestedSolutionStack(SolutionStack, NestedStack):
    """A nested version of SolutionStack"""

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
