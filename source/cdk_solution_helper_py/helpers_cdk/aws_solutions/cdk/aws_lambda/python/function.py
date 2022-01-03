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
from typing import List, Union

import aws_cdk.aws_iam as iam
from aws_cdk.aws_lambda import Function, Runtime, RuntimeFamily, Code
from aws_cdk.core import (
    Construct,
    AssetHashType,
    BundlingOptions,
    BundlingDockerImage,
    Aws,
)

from aws_solutions.cdk.aws_lambda.python.bundling import SolutionsPythonBundling
from aws_solutions.cdk.aws_lambda.python.directory_hash import DirectoryHash

DEFAULT_RUNTIME = Runtime.PYTHON_3_7
DEPENDENCY_EXCLUDES = ["*.pyc"]


class SolutionsPythonFunction(Function):
    """This is similar to aws-cdk/aws-lambda-python, however it handles local bundling"""

    def __init__(
        self,  # NOSONAR (python:S107) - allow large number of method parameters
        scope: Construct,
        construct_id: str,
        entrypoint: Path,
        function: str,
        libraries: Union[List[Path], Path, None] = None,
        **kwargs,
    ):
        self.scope = scope
        self.construct_id = construct_id
        self.source_path = entrypoint.parent

        # validate source path
        if not self.source_path.is_dir():
            raise ValueError(
                f"entrypoint {entrypoint} must not be a directory, but rather a .py file"
            )

        # validate libraries
        self.libraries = libraries or []
        self.libraries = (
            self.libraries if isinstance(self.libraries, list) else [self.libraries]
        )
        for lib in self.libraries:
            if lib.is_file():
                raise ValueError(
                    f"library {lib} must not be a file, but rather a directory"
                )

        # create default least privileged role for this function unless a role is passed
        if not kwargs.get("role"):
            kwargs["role"] = self._create_role()

        # python 3.7 is selected to support custom resources and inline code
        if not kwargs.get("runtime"):
            kwargs["runtime"] = DEFAULT_RUNTIME

        # validate that the user is using a python runtime for AWS Lambda
        if kwargs["runtime"].family != RuntimeFamily.PYTHON:
            raise ValueError(
                f"SolutionsPythonFunction must use a Python runtime ({kwargs['runtime']} was provided)"
            )

        # build the handler based on the entrypoint Path and function name
        if kwargs.get("handler"):
            raise ValueError(
                f"SolutionsPythonFunction expects a Path `entrypoint` (python file) and `function` (function in the entrypoint for AWS Lambda to invoke)"
            )
        else:
            kwargs["handler"] = f"{entrypoint.stem}.{function}"

        # build the code based on the entrypoint Path
        if kwargs.get("code"):
            raise ValueError(
                f"SolutionsPythonFunction expects a Path `entrypoint` (python file) and `function` (function in the entrypoint for AWS Lambda to invoke)"
            )

        bundling = SolutionsPythonBundling(
            self.source_path,
            self.libraries,
        )

        kwargs["code"] = self._get_code(bundling, runtime=kwargs["runtime"])

        # initialize the parent Function
        super().__init__(scope, construct_id, **kwargs)

    def _get_code(self, bundling: SolutionsPythonBundling, runtime: Runtime) -> Code:
        # try to create the code locally - if this fails, try using Docker
        code_parameters = {
            "path": str(self.source_path),
            "asset_hash_type": AssetHashType.CUSTOM,
            "asset_hash": DirectoryHash.hash(self.source_path, *self.libraries),
            "exclude": DEPENDENCY_EXCLUDES,
        }

        # to enable docker only bundling, use image=self._get_bundling_docker_image(bundling, runtime=runtime)
        code = Code.from_asset(
            bundling=BundlingOptions(
                image=BundlingDockerImage.from_registry(
                    "scratch"
                ),  # NOT USED - FOR NOW ALL BUNDLING IS LOCAL
                command=["NOT-USED"],
                entrypoint=["NOT-USED"],
                local=bundling,
            ),
            **code_parameters,
        )

        return code

    def _create_role(self) -> iam.Role:
        """
        Build a role that allows an AWS Lambda Function to log to CloudWatch
        :param name: The name of the role. The final name will be "{name}-Role"
        :return: aws_cdk.aws_iam.Role
        """
        return iam.Role(
            self.scope,
            f"{self.construct_id}-Role",
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
