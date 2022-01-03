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
from typing import Dict

from aws_cdk.core import (
    Construct,
    CfnResource,
    Fn,
    CfnCondition,
    Aws,
)

from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression


class Metrics(Construct):
    """Used to track anonymous solution deployment metrics."""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        metrics: Dict[str, str],
    ):
        super().__init__(scope, construct_id)

        if not isinstance(metrics, dict):
            raise ValueError("metrics must be a dictionary")

        self._metrics_function = SolutionsPythonFunction(
            self,
            "MetricsFunction",
            entrypoint=Path(__file__).parent
            / "src"
            / "custom_resources"
            / "metrics.py",
            function="handler",
        )
        add_cfn_nag_suppressions(
            resource=self._metrics_function.node.default_child,
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

        self._send_anonymous_usage_data = CfnCondition(
            self,
            "SendAnonymousUsageData",
            expression=Fn.condition_equals(
                Fn.find_in_map("Solution", "Data", "SendAnonymousUsageData"), "Yes"
            ),
        )
        self._send_anonymous_usage_data.override_logical_id("SendAnonymousUsageData")

        properties = {
            "ServiceToken": self._metrics_function.function_arn,
            "Solution": self.node.try_get_context("SOLUTION_NAME"),
            "Version": self.node.try_get_context("VERSION"),
            "Region": Aws.REGION,
            **metrics,
        }
        self.solution_metrics = CfnResource(
            self,
            "SolutionMetricsAnonymousData",
            type="Custom::AnonymousData",
            properties=properties,
        )
        self.solution_metrics.override_logical_id("SolutionMetricsAnonymousData")
        self.solution_metrics.cfn_options.condition = self._send_anonymous_usage_data
