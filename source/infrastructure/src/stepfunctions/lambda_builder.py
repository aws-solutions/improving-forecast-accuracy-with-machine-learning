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

import logging
from pathlib import Path
from typing import Dict, List

import aws_cdk.aws_iam as iam
from aws_cdk.aws_lambda import Runtime, Function, LayerVersion, Code
from aws_cdk.core import Construct, Duration, BundlingOptions, Aws

from interfaces.bundling import SolutionBundling

logger = logging.getLogger("cdk-helper")


class LambdaBuilder:
    def __init__(self, parent: Construct, log_level: str, source_path: Path):
        self.parent = parent
        self.log_level = log_level
        self.source_path = source_path
        self._bundling = {}

    def _runtime_source_path(self, runtime: Runtime):
        runtime = runtime.name
        if (
            runtime == "python3.8"
            or runtime == "python3.7"
            or runtime == "python3.6"
            or runtime == "python2.7"
        ):
            path = "python"
        else:
            logger.error("unsupported runtime %s" % runtime)
            raise ValueError(f"unsupported runtime {runtime}")
        return path

    def layer_for(self, name: str, base: str, runtimes: List[Runtime]):
        if len(runtimes) > 1:
            logger.error("multiple runtimes are not supported at this time")

        bundling = self._get_bundling(
            base, source_path=self._runtime_source_path(runtimes[0])
        )
        code = Code.from_asset(str(self.source_path), bundling=bundling)
        layer = LayerVersion(self.parent, name, code=code, compatible_runtimes=runtimes)
        return layer

    def functions_for(
        self,
        name,
        base,
        handlers,
        libs=None,
        timeout=Duration.minutes(5),
        runtime=Runtime.PYTHON_3_8,
        layers=None,
    ) -> Dict[str, Function]:
        if isinstance(handlers, str):
            handlers = [handlers]
        if not isinstance(handlers, list):
            raise ValueError("handlers must be a string or a list of handlers")
        if isinstance(libs, str):
            libs = [libs]
        if isinstance(layers, str):
            layers = [layers]
        if libs and not isinstance(libs, list):
            raise ValueError("libs must be a string or a list of libraries")

        bundling = self._get_bundling(base, libs=libs)
        code = Code.from_asset(str(self.source_path), bundling=bundling)
        role = self.build_lambda_logging_role(name)
        functions = {}
        for handler in handlers:
            handler_name = (
                handler.split(".")[0]
                .replace("_", " ")
                .title()
                .replace(" ", "")
                .replace("Handler", "")
            )
            if handler_name.startswith(name):
                func_name = handler_name
            else:
                func_name = name + handler_name

            function = Function(
                self.parent,
                func_name,
                handler=handler,
                code=code,
                runtime=runtime,
                timeout=timeout,
                role=role,
                layers=layers,
                environment={"LOG_LEVEL": self.log_level},
            )

            functions.update({func_name: function})
        return functions

    def _get_bundling(self, path, libs=None, source_path=""):
        if self._bundling.get(path):
            return self._bundling[path]

        libs = [] if not libs else libs
        libs = [libs] if isinstance(libs, str) else libs
        if not isinstance(libs, list):
            raise ValueError("libs must be a string or a list")

        # override the destination path as required (used in lambda functions)
        destination_path = Path("/asset-output")
        if source_path:
            destination_path = destination_path.joinpath(source_path)

        bundle_script = [
            f"echo '{path} bundling... started'",
            f"cp -r /asset-input/{path}/* /asset-output/",
            f'if [ -f "/asset-input/{path}/requirements.txt" ]; then echo \'{path} bundling... python requirements\' && pip install --no-cache-dir -t {destination_path} -r "/asset-input/{path}/requirements.txt" --no-color; fi',
        ]
        for lib in libs:
            bundle_script.extend(
                [
                    f"echo '{path} bundling... adding lib {lib}'",
                    f"cp -r /asset-input/{lib} /asset-output/",
                ]
            )
        bundle_script.append(f"echo '{path} bundling... completed'")

        command = ["bash", "-c", "&& ".join(bundle_script)]

        solutions_bundler = SolutionBundling(
            source_path=self.source_path,
            to_bundle=path,
            libs=libs,
            install_path=source_path,
        )
        bundling = BundlingOptions(
            image=Runtime.PYTHON_3_8.bundling_docker_image,
            command=command,
            local=solutions_bundler,
        )
        return bundling

    def build_lambda_logging_role(self, name) -> iam.Role:
        """
        Build a role that allows an AWS Lambda Function to log to CloudWatch
        :param name: The name of the role. The final name will be "{name}-Role"
        :return: aws_cdk.aws_iam.Role
        """
        return iam.Role(
            self.parent,
            f"{name}-Role",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            inline_policies={
                "LambdaFunctionServiceRolePolicy": iam.PolicyDocument(
                    statements=[
                        iam.PolicyStatement(
                            actions=[
                                "logs:CreateLogGroup",
                                "logs:CreateLogStream",
                                "logs:PutLogEvents",
                            ],
                            resources=[
                                f"arn:{Aws.PARTITION}:logs:{Aws.REGION}:{Aws.ACCOUNT_ID}:log-group:/aws/lambda/*"
                            ],
                        )
                    ]
                )
            },
        )
