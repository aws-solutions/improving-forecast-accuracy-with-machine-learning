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
from aws_cdk.core import Construct, CfnResource


class Metrics(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        metrics_function: IFunction,
        metrics: Dict[str, str],
    ):
        super().__init__(scope, id)

        properties = {"ServiceToken": metrics_function.function_arn, **metrics}
        self.solution_metrics = CfnResource(
            self,
            "SolutionMetricsAnonymousData",
            type="Custom::AnonymousData",
            properties=properties,
        )
        self.solution_metrics.override_logical_id("SolutionMetricsAnonymousData")
