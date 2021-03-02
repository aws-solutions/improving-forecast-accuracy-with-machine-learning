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
from typing import Dict

from aws_cdk.aws_lambda import IFunction
from aws_cdk.core import Construct, CfnResource, Fn, CfnCondition


class Metrics(Construct):
    """Used to track anonymous solution deployment metrics."""

    def __init__(
        self,
        scope: Construct,
        id: str,
        metrics_function: IFunction,
        metrics: Dict[str, str],
    ):
        super().__init__(scope, id)

        send_anonymous_usage_data = CfnCondition(
            self,
            "SendAnonymousUsageData",
            expression=Fn.condition_equals(
                Fn.find_in_map("Solution", "Data", "SendAnonymousUsageData"), "Yes"
            ),
        )

        properties = {"ServiceToken": metrics_function.function_arn, **metrics}
        self.solution_metrics = CfnResource(
            self,
            "SolutionMetricsAnonymousData",
            type="Custom::AnonymousData",
            properties=properties,
        )
        self.solution_metrics.override_logical_id("SolutionMetricsAnonymousData")
        self.solution_metrics.cfn_options.condition = send_anonymous_usage_data
