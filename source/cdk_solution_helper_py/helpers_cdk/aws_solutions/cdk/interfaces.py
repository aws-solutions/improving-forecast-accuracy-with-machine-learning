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

import logging
from dataclasses import dataclass
from typing import Union, List

import jsii
from aws_cdk.core import (
    ITemplateOptions,
    Stack,
    NestedStack,
    CfnParameter,
)

logger = logging.getLogger("cdk-helper")


@dataclass
class _TemplateParameter:
    """Stores information about a CloudFormation parameter, its label (description) and group"""

    name: str
    label: str
    group: str


class TemplateOptionsException(Exception):
    pass


@jsii.implements(ITemplateOptions)
class TemplateOptions:
    """Helper class for setting up template CloudFormation parameter groups, labels and solutions metadata"""

    _metadata = {}

    def __init__(
        self,
        stack: Union[Stack, NestedStack],
        construct_id: str,
        description: str,
        filename: str,
    ):
        self.stack = stack
        self.filename = filename
        self._parameters: List[_TemplateParameter] = []
        self.stack.template_options.description = description
        self.stack.template_options.metadata = self.metadata

        self._metadata = self._get_metadata()

        if not filename.endswith(".template"):
            raise TemplateOptionsException("template filenames must end with .template")

        # if this stack is a nested stack, record its CDK ID in the parent stack's resource to it
        if getattr(stack, "nested_stack_resource"):
            stack.nested_stack_resource.add_metadata(
                "aws:solutions:templateid", construct_id
            )
            stack.nested_stack_resource.add_metadata(
                "aws:solutions:templatename", filename
            )

    @property
    def metadata(self) -> dict:
        return self._metadata

    def _get_metadata(self) -> dict:
        pgs = set()
        parameter_groups = [
            p.group
            for p in self._parameters
            if p.group not in pgs and not pgs.add(p.group)
        ]
        metadata = {
            "AWS::CloudFormation::Interface": {
                "ParameterGroups": [
                    {
                        "Label": {"default": parameter_group},
                        "Parameters": [
                            parameter.name
                            for parameter in self._parameters
                            if parameter.group == parameter_group
                        ],
                    }
                    for parameter_group in parameter_groups
                ],
                "ParameterLabels": {
                    parameter.name: {"default": parameter.label}
                    for parameter in self._parameters
                },
            },
            "aws:solutions:templatename": self.filename,
        }
        self.stack.template_options.metadata = metadata
        return metadata

    def add_parameter(self, parameter: CfnParameter, label: str, group: str):
        self._parameters.append(_TemplateParameter(parameter.logical_id, label, group))
        self._metadata = self._get_metadata()
