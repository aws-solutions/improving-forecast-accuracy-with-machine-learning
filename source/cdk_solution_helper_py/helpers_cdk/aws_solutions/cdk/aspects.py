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
import jsii
from aws_cdk.core import CfnCondition, IAspect, IConstruct


@jsii.implements(IAspect)
class ConditionalResources:
    """Mark any CDK construct as conditional (this is useful to apply to stacks and L2+ constructs)"""

    def __init__(self, condition: CfnCondition):
        self.condition = condition

    def visit(self, node: IConstruct):
        if "is_cfn_element" in dir(node) and node.is_cfn_element(node):
            node.cfn_options.condition = self.condition
        elif "is_cfn_element" in dir(node.node.default_child):
            node.node.default_child.cfn_options.condition = self.condition
