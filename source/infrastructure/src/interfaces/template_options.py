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

import jsii
from aws_cdk.core import ITemplateOptions


@jsii.implements(ITemplateOptions)
class TemplateOptions:
    _metadata = {}

    @property
    def metadata(self) -> dict:
        return self._metadata

    def _prepare_metadata(self):
        if not self._metadata:
            self._metadata = {
                "AWS::CloudFormation::Interface": {
                    "ParameterGroups": [],
                    "ParameterLabels": {},
                }
            }

    def add_parameter_group(self, label, parameters):
        self._prepare_metadata()
        self._metadata["AWS::CloudFormation::Interface"]["ParameterGroups"].append(
            {
                "Label": {"default": label},
                "Parameters": [parameter.node.id for parameter in parameters],
            }
        )

    def add_parameter_label(self, parameter, label):
        self._prepare_metadata()
        self._metadata["AWS::CloudFormation::Interface"]["ParameterLabels"][
            parameter.node.id
        ] = {"default": label}
