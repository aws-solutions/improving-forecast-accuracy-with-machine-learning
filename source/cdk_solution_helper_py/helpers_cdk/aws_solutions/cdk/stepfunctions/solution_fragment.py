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
from typing import List, Dict
from typing import Optional

from aws_cdk.aws_lambda import CfnFunction
from aws_cdk.aws_stepfunctions import State, INextable, TaskInput, StateMachineFragment
from aws_cdk.aws_stepfunctions_tasks import LambdaInvoke
from aws_cdk.core import Construct, Duration


class SolutionFragment(StateMachineFragment):
    def __init__(
        self,  # NOSONAR (python:S107) - allow large number of method parameters
        scope: Construct,
        id: str,
        function: CfnFunction,
        payload: Optional[TaskInput] = None,
        input_path: Optional[str] = "$",
        result_path: Optional[str] = "$",
        output_path: Optional[str] = "$",
        result_selector: Optional[Dict] = None,
        failure_state: Optional[State] = None,
        backoff_rate: Optional[int] = 1.05,
        interval: Optional[Duration] = Duration.seconds(5),
        max_attempts: Optional[int] = 5,
    ):
        super().__init__(scope, id)

        self.failure_state = failure_state

        self.task = LambdaInvoke(
            self,
            id,
            lambda_function=function,
            retry_on_service_exceptions=True,
            input_path=input_path,
            result_path=result_path,
            output_path=output_path,
            payload=payload,
            payload_response_only=True,
            result_selector=result_selector,
        )
        self.task.add_retry(
            backoff_rate=backoff_rate,
            interval=interval,
            max_attempts=max_attempts,
            errors=["ResourcePending"],
        )
        if self.failure_state:
            self.task.add_catch(
                failure_state,
                errors=["ResourceFailed", "ResourceInvalid"],
                result_path="$.statesError",
            )
            self.task.add_catch(
                failure_state, errors=["States.ALL"], result_path="$.statesError"
            )

    @property
    def start_state(self) -> State:
        return self.task

    @property
    def end_states(self) -> List[INextable]:
        """
        Get the end states of this chain
        :return: The chainable end states of this chain (i.e. not the failure state)
        """
        states = [self.task]
        return states
