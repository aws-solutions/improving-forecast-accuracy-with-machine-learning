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
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Dict

import aws_cdk.aws_iam as iam
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
from aws_cdk.aws_lambda import Runtime, Function, LayerVersion, Code, IFunction
from aws_cdk.aws_s3 import IBucket
from aws_cdk.core import (
    Construct,
    BundlingOptions,
    Duration,
    Aws,
    CfnParameter,
    CfnResource,
    CfnMapping,
)

from interfaces.bundling import SolutionBundling
from solutions.cfn_nag import CfnNagSuppression, add_cfn_nag_suppressions


@dataclass
class Policies:
    owner: Construct
    _forecast_access_roles: Dict[str, iam.Role] = field(
        init=False, default_factory=dict
    )

    def forecast_s3_access_role(
        self, name: str, data_bucket_name_resource: CfnResource
    ):
        try:
            return self._forecast_access_roles[name]
        except KeyError:
            self._forecast_access_roles[name] = iam.Role(
                self.owner,
                f"{name}ForecastAccessRole",
                assumed_by=iam.ServicePrincipal("forecast.amazonaws.com"),
                inline_policies={
                    "ForecastS3AccessRole": iam.PolicyDocument(
                        statements=[
                            iam.PolicyStatement(
                                effect=iam.Effect.ALLOW,
                                actions=["s3:Get*", "s3:List*", "s3:PutObject",],
                                resources=self.data_bucket_resources(
                                    data_bucket_name_resource
                                ),
                            )
                        ]
                    )
                },
            )
            return self._forecast_access_roles[name]

    def data_bucket_resources(self, data_bucket_name_resource: CfnResource):
        data_bucket_name = data_bucket_name_resource.get_att("Name").to_string()
        return [
            f"arn:{Aws.PARTITION}:s3:::{data_bucket_name}/*",
            f"arn:{Aws.PARTITION}:s3:::{data_bucket_name}",
        ]

    def s3_bucket_access_policy(self, name: str, data_bucket_name_resource: str):
        return iam.Policy(
            self.owner,
            f"{name}S3BucketAccess",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["s3:GetObject", "s3:ListBucket", "s3:ListObjects",],
                    resources=self.data_bucket_resources(data_bucket_name_resource),
                )
            ],
        )

    def forecast_access_policy(self, name):
        policy = iam.Policy(
            self.owner,
            f"{name}ForecastAccess",
            statements=[
                iam.PolicyStatement(
                    actions=[
                        "forecast:Describe*",
                        "forecast:List*",
                        "forecast:Create*",
                        "forecast:Update*",
                        "forecast:TagResource",
                    ],
                    resources=["*"],
                )
            ],
        )
        add_cfn_nag_suppressions(
            policy.node.default_child,
            [
                CfnNagSuppression(
                    "W12",
                    "Require access to all resources; Not all Amazon Forecast resources support resource based policy",
                )
            ],
        )
        return policy

    def athena_access(self, workgroup_name):
        return iam.Policy(
            self.owner,
            "AthenaAccess",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "athena:StartQueryExecution",
                        "athena:GetQueryExecution",
                        "athena:GetQueryResults",
                    ],
                    resources=[
                        f"arn:{Aws.PARTITION}:athena:{Aws.REGION}:{Aws.ACCOUNT_ID}:workgroup/{workgroup_name}"
                    ],
                )
            ],
        )

    def glue_access(
        self,
        catalog: CfnResource,
        athena_bucket: IBucket,
        data_bucket_name_resource: str,
    ):
        return iam.Policy(
            self.owner,
            "GlueAccess",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "glue:GetDatabase",
                        "glue:GetTable",
                        "glue:GetPartitions",
                        "glue:DeleteTable",  # required to delete temporary tables
                        "glue:CreateTable",
                        "glue:BatchCreatePartition",
                    ],
                    resources=[
                        f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:catalog",
                        f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:{catalog.ref}",
                        f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:table/{catalog.ref}/*",
                        f"arn:{Aws.PARTITION}:glue:{Aws.REGION}:{Aws.ACCOUNT_ID}:database/{catalog.ref}",
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:ListObjects",
                        "s3:PutObject",
                        "s3:CreateMultipartUpload",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload",
                    ],
                    resources=[
                        athena_bucket.arn_for_objects("*"),
                        athena_bucket.bucket_arn,
                    ],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "s3:GetBucketLocation",
                        "s3:GetObject",
                        "s3:ListBucket",
                        "s3:ListObjects",
                        "s3:PutObject",
                        "s3:CreateMultipartUpload",
                        "s3:ListMultipartUploadParts",
                        "s3:AbortMultipartUpload",
                    ],
                    resources=self.data_bucket_resources(data_bucket_name_resource),
                ),
            ],
        )

    def quicksight_access(self):
        policy = iam.Policy(
            self.owner,
            "QuickSightAccess",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "quicksight:CreateAnalysis",
                        "quicksight:CreateDataSet",
                        "quicksight:CreateDataSource",
                        "quicksight:UpdateDataSource",
                        "quicksight:UpdateDataSourcePermissions",
                        "quicksight:Describe*",
                        "quicksight:Get*",
                        "quicksight:List*",
                        "quicksight:PassDataSet",
                        "quicksight:PassDataSource",
                        "quicksight:RestoreAnalysis",
                        "quicksight:SearchAnalyses",
                    ],
                    resources=["*"],
                )
            ],
        )
        add_cfn_nag_suppressions(
            policy.node.default_child,
            [
                CfnNagSuppression(
                    "W12",
                    "Require access to all resources; Not all Amazon Forecast resources support resource based policy",
                )
            ],
        )

        return policy


class LambdaFunctions(Construct):
    def __init__(self, scope: Construct, id: str, log_level: CfnParameter):
        super().__init__(scope, id)
        self._bundling = {}
        self.log_level = log_level.value_as_string
        self.source_path = Path(__file__).parent.parent.parent.parent
        self.topic = None
        self.subscription = None
        self.functions: Dict[Function] = {}
        self.policies = Policies(self)
        self.create_functions()

        # step function steps
        check_error = sfn.Choice(self, "Check-Error")
        notify_failed = tasks.LambdaInvoke(
            self,
            "Notify-Failed",
            lambda_function=self.functions["SNS"],
            payload_response_only=True,
            retry_on_service_exceptions=True,
            result_path=None,
        )
        notify_failed.next(sfn.Fail(self, "FailureState"))

        create_dataset_group = tasks.LambdaInvoke(
            self,
            "Create-DatasetGroup",
            lambda_function=self.functions["CreateDatasetGroup"],
            result_path="$.DatasetGroupNames",
            payload_response_only=True,
            retry_on_service_exceptions=True,
        )
        create_dataset_group.add_retry(
            backoff_rate=1.05, interval=Duration.seconds(5), errors=["ResourcePending"]
        )
        create_dataset_group.add_catch(
            notify_failed, errors=["ResourceFailed"], result_path="$.serviceError"
        )
        create_dataset_group.add_catch(
            notify_failed, errors=["States.ALL"], result_path="$.statesError"
        )

        import_data = tasks.LambdaInvoke(
            self,
            "Import-Data",
            lambda_function=self.functions["CreateDatasetImportJob"],
            result_path="$.DatasetImportJobArn",
            payload_response_only=True,
            retry_on_service_exceptions=True,
        )
        import_data.add_retry(
            backoff_rate=1.05,
            interval=Duration.seconds(5),
            max_attempts=100,
            errors=["ResourcePending"],
        )
        import_data.add_catch(
            notify_failed, errors=["ResourceFailed"], result_path="$.serviceError"
        )
        import_data.add_catch(
            notify_failed, errors=["States.ALL"], result_path="$.statesError"
        )

        update_not_required = sfn.Succeed(self, "Update-Not-Required")
        notify_success = tasks.LambdaInvoke(
            self,
            "Notify-Success",
            lambda_function=self.functions["SNS"],
            payload_response_only=True,
            retry_on_service_exceptions=True,
            result_path=None,
        )

        notify_prediction_failed = tasks.LambdaInvoke(
            self,
            "Notify-Prediction-Failed",
            lambda_function=self.functions["SNS"],
            payload_response_only=True,
            retry_on_service_exceptions=True,
            result_path=None,
        )
        notify_prediction_failed.next(sfn.Fail(self, "Prediction-Failed"))

        create_predictor = tasks.LambdaInvoke(
            self,
            "Create-Predictor",
            lambda_function=self.functions["CreatePredictor"],
            result_path="$.PredictorArn",
            payload_response_only=True,
            retry_on_service_exceptions=True,
        )
        create_predictor.add_retry(
            backoff_rate=1.05,
            interval=Duration.seconds(5),
            max_attempts=100,
            errors=["ResourcePending", "DatasetsImporting"],
        )
        create_predictor.add_catch(
            notify_prediction_failed,
            errors=["ResourceFailed"],
            result_path="$.serviceError",
        )
        create_predictor.add_catch(
            notify_prediction_failed, errors=["States.ALL"], result_path="$.statesError"
        )
        create_predictor.add_catch(update_not_required, errors=["NotMostRecentUpdate"])

        create_forecast = tasks.LambdaInvoke(
            self,
            "Create-Forecast",
            lambda_function=self.functions["CreateForecast"],
            result_path="$.ForecastArn",
            payload_response_only=True,
            retry_on_service_exceptions=True,
        )
        create_forecast.add_retry(
            backoff_rate=1.05,
            interval=Duration.seconds(5),
            max_attempts=100,
            errors=["ResourcePending"],
        )
        create_forecast.add_catch(
            notify_prediction_failed,
            errors=["ResourceFailed"],
            result_path="$.serviceError",
        )
        create_forecast.add_catch(
            notify_prediction_failed, errors=["States.ALL"], result_path="$.statesError"
        )

        export_forecast = tasks.LambdaInvoke(
            self,
            "Export-Forecast",
            lambda_function=self.functions["PrepareForecastExport"],
            result_path="$.ExportTableName",
            payload_response_only=True,
            retry_on_service_exceptions=True,
        )
        export_forecast.add_catch(
            notify_prediction_failed,
            errors=["ResourceFailed"],
            result_path="$.serviceError",
        )
        export_forecast.add_catch(
            notify_prediction_failed, errors=["States.ALL"], result_path="$.statesError"
        )

        create_forecasts = sfn.Map(
            self,
            "Create-Forecasts",
            items_path="$.DatasetGroupNames",
            parameters={
                "bucket.$": "$.bucket",
                "dataset_file.$": "$.dataset_file",
                "dataset_group_name.$": "$$.Map.Item.Value",
                "config.$": "$.config",
            },
        )

        # step function definition
        definition = (
            check_error.when(sfn.Condition.is_present("$.serviceError"), notify_failed)
            .otherwise(create_dataset_group)
            .afterwards()
            .next(import_data)
            .next(
                create_forecasts.iterator(
                    create_predictor.next(create_forecast)
                    .next(export_forecast)
                    .next(notify_success)
                )
            )
        )

        self.state_machine = sfn.StateMachine(
            self, "DeployStateMachine", definition=definition
        )

    def set_forecast_permissions(self, name, data_bucket_name_resource: CfnResource):
        function = self.functions[name]
        function.role.attach_inline_policy(self.policies.forecast_access_policy(name))
        function.role.attach_inline_policy(
            self.policies.s3_bucket_access_policy(name, data_bucket_name_resource)
        )

    def set_s3_notification_permissions(self, data_bucket_name_resource: CfnResource):
        self.state_machine.grant_start_execution(self.functions["S3NotificationLambda"])
        self.functions["S3NotificationLambda"].add_environment(
            "STATE_MACHINE_ARN", self.state_machine.state_machine_arn
        )
        self.functions["S3NotificationLambda"].role.attach_inline_policy(
            self.policies.s3_bucket_access_policy(
                "S3NotificationLambda", data_bucket_name_resource
            )
        )

    def set_forecast_s3_access_permissions(
        self, name, function: IFunction, data_bucket_name_resource: CfnResource
    ):
        forecast_s3_access_role = self.policies.forecast_s3_access_role(
            name=name, data_bucket_name_resource=data_bucket_name_resource
        )
        function.role.attach_inline_policy(
            iam.Policy(
                self,
                f"{function.node.id}ForecastPassRolePolicy",
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["iam:PassRole"],
                        resources=[forecast_s3_access_role.role_arn],
                    )
                ],
            )
        )
        function.add_environment("FORECAST_ROLE", forecast_s3_access_role.role_arn)

    def set_forecast_etl_permissions(
        self,
        function: IFunction,
        database: CfnResource,
        workgroup: CfnResource,
        quicksight_principal: CfnParameter,
        quicksight_source: CfnMapping,
        athena_bucket: IBucket,
        data_bucket_name_resource: CfnResource,
    ):
        function.role.attach_inline_policy(self.policies.athena_access(workgroup.ref))
        function.role.attach_inline_policy(
            self.policies.glue_access(
                catalog=database,
                athena_bucket=athena_bucket,
                data_bucket_name_resource=data_bucket_name_resource,
            )
        )
        function.role.attach_inline_policy(self.policies.quicksight_access())
        function.add_environment("SCHEMA_NAME", database.ref)
        function.add_environment("WORKGROUP_NAME", workgroup.ref)
        function.add_environment(
            "QUICKSIGHT_PRINCIPAL", quicksight_principal.value_as_string
        )
        function.add_environment(
            "QUICKSIGHT_SOURCE",
            quicksight_source.find_in_map("General", "QuickSightSourceTemplateArn"),
        )

    def create_functions(self):
        """
        Create all AWS Lambda functions used by the solution
        :return: None
        """
        layer_deps = self.layer_for(
            name="Dependencies",
            base="lambdas/lambda_dependencies",
            runtimes=[Runtime.PYTHON_3_8],
        )

        layer_data = self.layer_for(
            name="DatasetUtils",
            base="lambdas/lambda_datasetutils",
            runtimes=[Runtime.PYTHON_3_8],
        )

        self.functions.update(
            self.functions_for(
                name="S3NotificationLambda",
                base="lambdas/notification",
                handlers="handler.notification",
                libs="shared",
                layers=[layer_deps],
            )
        )
        self.functions["S3NotificationLambda"].add_permission(
            "S3NotificationLambdaS3BucketPermission",
            action="lambda:InvokeFunction",
            source_account=Aws.ACCOUNT_ID,
            principal=iam.ServicePrincipal("s3.amazonaws.com"),
        )

        self.functions.update(
            self.functions_for(
                name="CreateDatasetGroup",
                base="lambdas/createdatasetgroup",
                handlers="handler.createdatasetgroup",
                libs="shared",
                layers=[layer_deps],
            )
        )

        self.functions.update(
            self.functions_for(
                name="CreateDatasetImportJob",
                base="lambdas/createdatasetimportjob",
                handlers="handler.createdatasetimportjob",
                libs="shared",
                layers=[layer_deps],
            )
        )

        self.functions.update(
            self.functions_for(
                name="CreateForecast",
                base="lambdas/createforecast",
                handlers="handler.createforecast",
                libs="shared",
                layers=[layer_deps],
            )
        )

        self.functions.update(
            self.functions_for(
                name="CreatePredictor",
                base="lambdas/createpredictor",
                handlers="handler.createpredictor",
                libs="shared",
                layers=[layer_deps],
            )
        )

        self.functions.update(
            self.functions_for(
                name="PrepareForecastExport",
                base="lambdas/datasetutils",
                handlers="handler.prepareexport",
                libs="shared",
                timeout=Duration.minutes(15),
                layers=[layer_data],
            )
        )

        self.functions.update(
            self.functions_for(
                name="SNS",
                base="lambdas/sns",
                handlers="handler.sns",
                libs="shared",
                layers=[layer_deps],
            )
        )

        self.functions.update(
            self.functions_for(
                name="CfnResource",
                base="lambdas/cloudformation_resources",
                handlers=[
                    "bucket_name.handler",
                    "solution_metrics.handler",
                    "unique_name.handler",
                ],
                timeout=Duration.seconds(10),
            )
        )

    def layer_for(self, name: str, base: str, runtimes: List[Runtime]):
        bundling = self._get_bundling(
            base, source_path="python/lib/python3.8/site-packages"
        )
        code = Code.from_asset(str(self.source_path), bundling=bundling)
        layer = LayerVersion(self, name, code=code, compatible_runtimes=runtimes)
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
        role = self.build_lambda_role(name)
        functions = {}
        for handler in handlers:
            func_name = name + handler.split(".")[0].replace("_", " ").title().replace(
                " ", ""
            ).replace("Handler", "")
            functions.update(
                {
                    func_name: Function(
                        self,
                        func_name,
                        handler=handler,
                        code=code,
                        runtime=runtime,
                        timeout=timeout,
                        role=role,
                        layers=layers,
                        environment={"LOG_LEVEL": self.log_level},
                    )
                }
            )
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

    def build_lambda_role(self, name) -> iam.Role:
        return iam.Role(
            self,
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
