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

from dataclasses import dataclass
from typing import List

from aws_cdk.core import CfnResource


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
