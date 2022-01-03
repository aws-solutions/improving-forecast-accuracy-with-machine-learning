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

from dataclasses import dataclass
from typing import List

import jsii
from aws_cdk.core import CfnResource, IAspect, IConstruct


@dataclass
class CfnNagSuppression:
    rule_id: str
    reason: str


def add_cfn_nag_suppressions(
    resource: CfnResource, suppressions: List[CfnNagSuppression]
):
    resource.add_metadata(
        "cfn_nag",
        {
            "rules_to_suppress": [
                {"id": suppression.rule_id, "reason": suppression.reason}
                for suppression in suppressions
            ]
        },
    )


@jsii.implements(IAspect)
class CfnNagSuppressAll:
    """Suppress certain cfn_nag warnings that can be ignored by this solution"""

    def __init__(self, suppress: List[CfnNagSuppression], resource_type: str):
        self.suppressions = suppress
        self.resource_type = resource_type

    def visit(self, node: IConstruct):
        if "is_cfn_element" in dir(node) and node.is_cfn_element(node):
            if getattr(node, "cfn_resource_type", None) == self.resource_type:
                add_cfn_nag_suppressions(node, self.suppressions)

        elif "is_cfn_element" in dir(node.node.default_child) and (
            getattr(node.node.default_child, "cfn_resource_type", None)
            == self.resource_type
        ):
            add_cfn_nag_suppressions(node.node.default_child, self.suppressions)
