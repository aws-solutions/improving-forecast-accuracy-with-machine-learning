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

from typing import Union

import jsii
from aws_cdk.core import ITemplateOptions, Stack, NestedStack


@jsii.implements(ITemplateOptions)
class TemplateOptions:
    """Helper class for setting up template CloudFormation parameter groups, labels and solutions metadata"""

    _metadata = {}

    def __init__(
        self, stack: Union[Stack, NestedStack], id: str, description: str, filename: str
    ):
        self.stack = stack
        self._metadata = {
            "AWS::CloudFormation::Interface": {
                "ParameterGroups": [],
                "ParameterLabels": {},
            },
            "aws:solutions:templatename": filename,
        }
        self.stack.template_options.description = description
        self.stack.template_options.metadata = self.metadata

        # if this stack is a nested stack, record its CDK ID in the parent stack's resource to it
        if getattr(stack, "nested_stack_resource"):
            stack.nested_stack_resource.add_metadata("aws:solutions:templateid", id)
            stack.nested_stack_resource.add_metadata(
                "aws:solutions:templatename", filename
            )

    @property
    def metadata(self) -> dict:
        return self._metadata

    def add_parameter_group(self, label, parameters):
        self._metadata["AWS::CloudFormation::Interface"]["ParameterGroups"].append(
            {
                "Label": {"default": label},
                "Parameters": [parameter.node.id for parameter in parameters],
            }
        )
        self.stack.template_options.metadata = self.metadata

    def add_parameter_label(self, parameter, label):
        self._metadata["AWS::CloudFormation::Interface"]["ParameterLabels"][
            parameter.node.id
        ] = {"default": label}
        self.stack.template_options.metadata = self.metadata
