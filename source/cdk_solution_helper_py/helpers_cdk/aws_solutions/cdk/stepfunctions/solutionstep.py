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

from pathlib import Path
from typing import Optional, List

from constructs import Construct
from aws_cdk.aws_events import EventBus
from aws_cdk.aws_lambda import Tracing, Runtime, RuntimeFamily
from aws_cdk.aws_stepfunctions import IChainable, TaskInput, State
from aws_cdk import Duration

from aws_solutions.cdk.aws_lambda.environment import Environment
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression
from aws_solutions.cdk.stepfunctions.solution_fragment import SolutionFragment


class SolutionStep(Construct):
    def __init__(
        self,  # NOSONAR (python:S107) - allow large number of method parameters
        scope: Construct,
        id: str,
        function: str = "lambda_handler",
        entrypoint: Path = None,
        input_path: str = "$",
        result_path: str = "$",
        output_path: str = "$",
        payload: Optional[TaskInput] = None,
        layers=None,
        failure_state: Optional[IChainable] = None,
        libraries: Optional[List[Path]] = None,
        **kwargs,
    ):
        super().__init__(scope, f"{id} Solution Step")

        self.function = self._CreateLambdaFunction(
            self,
            f"{self._snake_case(id)}_fn",
            layers=layers,
            function=function,
            entrypoint=entrypoint,
            libraries=libraries,
            **kwargs,
        )
        add_cfn_nag_suppressions(
            self.function.role.node.try_find_child("DefaultPolicy").node.find_child(
                "Resource"
            ),
            [
                CfnNagSuppression(
                    "W12", "IAM policy for AWS X-Ray requires an allow on *"
                ),                 
                CfnNagSuppression(
                    "W76",
                    "SPCM for IAM policy document is higher than 25",
                )
            ],
        )

        self._input_path = input_path
        self._result_path = result_path
        self._output_path = output_path
        self._payload = payload
        self._failure_state = failure_state

        self._create_resources()
        self._set_permissions()
        self.environment = self._set_environment()

    def state(
        self,  # NOSONAR (python:S107) - allow large number of method parameters
        scope: Construct,
        construct_id,
        payload: Optional[TaskInput] = None,
        input_path: Optional[str] = None,
        result_path: Optional[str] = None,
        result_selector: Optional[str] = None,
        output_path: Optional[str] = None,
        failure_state: Optional[State] = None,
        **kwargs,
    ):
        payload = payload or self._payload
        input_path = input_path or self._input_path
        result_path = result_path or self._result_path
        output_path = output_path or self._output_path
        failure_state = failure_state or self._failure_state

        return SolutionFragment(
            scope,
            construct_id,
            function=self.function,
            payload=payload,
            input_path=input_path,
            result_path=result_path,
            output_path=output_path,
            failure_state=failure_state,
            result_selector=result_selector,
            **kwargs,
        )

    def _snake_case(self, name) -> str:
        return name.replace(" ", "_").lower()

    def _set_permissions(self) -> None:
        raise NotImplementedError("please implement _set_permissions")

    def grant_put_events(self, bus: EventBus):
        self.function.add_environment("EVENT_BUS_ARN", bus.event_bus_arn)
        bus.grant_put_events_to(self.function)

    def _create_resources(self) -> None:
        pass  # not required

    def _set_environment(self) -> Environment:
        return Environment(self.function)

    class _CreateLambdaFunction(SolutionsPythonFunction):
        def __init__(self, scope: Construct, construct_id: str, **kwargs):
            entrypoint = kwargs.pop("entrypoint", None)
            if not entrypoint or not entrypoint.exists():
                raise ValueError("an entrypoint (Path to a .py file) must be provided")

            libraries = kwargs.pop("libraries", None)
            if libraries and any(not l.exists() for l in libraries):
                raise ValueError(f"libraries provided, but do not exist at {libraries}")

            _function = kwargs.pop("function")
            kwargs["layers"] = kwargs.get("layers", [])
            kwargs["tracing"] = Tracing.ACTIVE
            kwargs["timeout"] = kwargs.get("timeout", Duration.seconds(15))
            kwargs["runtime"] = Runtime("python3.9", RuntimeFamily.PYTHON)

            super().__init__(
                scope,
                construct_id,
                entrypoint,
                _function,
                libraries=libraries,
                **kwargs,
            )
