# ######################################################################################################################
#  Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.                                                  #
#                                                                                                                      #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance      #
#  with the License. You may obtain a copy of the License at                                                           #
#                                                                                                                      #
#   http://www.apache.org/licenses/LICENSE-2.0                                                                         #
#                                                                                                                      #
#  Unless required by applicable law or agreed to in writing, software distributed under the License is distributed    #
#  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for   #
#  the specific language governing permissions and limitations under the License.                                      #
# ######################################################################################################################

from dataclasses import dataclass, field

from aws_cdk.aws_lambda import IFunction
from aws_cdk import Aws

from aws_solutions.cdk.aws_lambda.environment_variable import EnvironmentVariable


@dataclass
class Environment:
    """
    Tracks environment variables common to AWS Lambda functions deployed by this solution
    """

    scope: IFunction
    solution_name: EnvironmentVariable = field(init=False, repr=False)
    solution_id: EnvironmentVariable = field(init=False, repr=False)
    solution_version: EnvironmentVariable = field(init=False, repr=False)
    log_level: EnvironmentVariable = field(init=False, repr=False)
    powertools_service_name: EnvironmentVariable = field(init=False, repr=False)

    def __post_init__(self):
        cloudwatch_namespace_id = f"forecast_solution_{Aws.STACK_NAME}"
        cloudwatch_service_id_default = f"Workflow"

        self.solution_name = EnvironmentVariable(self.scope, "SOLUTION_NAME")
        self.solution_id = EnvironmentVariable(self.scope, "SOLUTION_ID")
        self.solution_version = EnvironmentVariable(self.scope, "SOLUTION_VERSION")
        self.log_level = EnvironmentVariable(self.scope, "LOG_LEVEL", "INFO")
        self.powertools_service_name = EnvironmentVariable(
            self.scope, "POWERTOOLS_SERVICE_NAME", cloudwatch_service_id_default
        )
        self.powertools_metrics_namespace = EnvironmentVariable(
            self.scope, "POWERTOOLS_METRICS_NAMESPACE", cloudwatch_namespace_id
        )
