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
from typing import Optional

import aws_cdk.aws_iam as iam
from aws_cdk.aws_lambda import Function, Runtime, RuntimeFamily, Code
from aws_cdk.core import (
    Construct,
    BundlingOptions,
    BundlingDockerImage,
    BundlingOutput,
    Aws,
)

from aws_solutions.cdk.aws_lambda.java.bundling import SolutionsJavaBundling


class SolutionsJavaFunction(Function):
    """This is similar to aws-cdk/aws-lambda-python, however it handles local building of Java Lambda Functions"""

    def __init__(
        self,  # NOSONAR
        scope: Construct,
        construct_id: str,
        project_path: Path,
        distribution_path: str,
        gradle_task: str,
        gradle_test: Optional[str] = None,
        **kwargs,
    ):
        self.scope = scope
        self.construct_id = construct_id
        self.project_path = project_path
        self.gradle_task = gradle_task
        self.gradle_test = gradle_test

        if not project_path.is_dir():
            raise ValueError(
                f"project_path {project_path} must be a directory, not a file"
            )

        # create default least privileged role for this function unless a role is passed
        if not kwargs.get("role"):
            kwargs["role"] = self._create_role()

        # Java 11 is the default runtime (Lambda supports 8/ 11)
        if not kwargs.get("runtime"):
            kwargs["runtime"] = Runtime.JAVA_11

        if kwargs["runtime"].family != RuntimeFamily.JAVA:
            raise ValueError(
                f"SolutionsJavaFunction must use a Java runtime ({kwargs['runtime']} was provided)"
            )

        # This Construct will handle the creation of the 'code' parameter
        if kwargs.get("code"):
            raise ValueError(
                f"SolutionsJavaFunction expects a Path `project_path` (python file) and `function` (function in the entrypoint for AWS Lambda to invoke)"
            )

        bundling = SolutionsJavaBundling(
            to_bundle=project_path,
            gradle_task=gradle_task,
            gradle_test=gradle_test,
            distribution_path=distribution_path,
        )

        kwargs["code"] = Code.from_asset(
            path=str(project_path),
            bundling=BundlingOptions(
                image=BundlingDockerImage.from_registry("scratch"),  # NOT USED
                command=["NOT-USED"],
                entrypoint=["NOT-USED"],
                local=bundling,
                output_type=BundlingOutput.ARCHIVED,
            ),
        )
        super().__init__(scope, construct_id, **kwargs)

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
