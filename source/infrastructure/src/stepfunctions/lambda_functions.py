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

import aws_cdk.aws_iam as iam
import aws_cdk.aws_stepfunctions as sfn
import aws_cdk.aws_stepfunctions_tasks as tasks
from aws_cdk.aws_lambda import Runtime, Function, IFunction
from aws_cdk.aws_s3 import IBucket
from aws_cdk.core import (
    Construct,
    Duration,
    Aws,
    CfnParameter,
    CfnResource,
    CfnMapping,
    CfnCondition,
)

from solutions.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression
from stepfunctions.lambda_builder import LambdaBuilder
from stepfunctions.policies import SfnPolicies


class LambdaFunctions(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        log_level: CfnParameter,
        forecast_kms: CfnCondition,
        forecast_kms_key_arn: str,
    ):
        super().__init__(scope, id)
        self.topic = None
        self.subscription = None
        self.functions: Dict[Function] = {}
        self.policies = SfnPolicies(self)
        self.forecast_kms = forecast_kms
        self.forecast_kms_key_arn = forecast_kms_key_arn

        # create any policies that can be created upfront
        self.policies.create_forecast_kms_read_policy(
            forecast_kms, forecast_kms_key_arn
        )
        self.policies.create_forecast_kms_write_policy(
            forecast_kms, forecast_kms_key_arn
        )

        # build functions
        self.lambda_builder = LambdaBuilder(
            self,
            log_level=log_level.value_as_string,
            source_path=Path(__file__).parent.parent.parent.parent,
        )

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
            backoff_rate=1.05,
            interval=Duration.seconds(5),
            errors=["ResourcePending"],
        )
        create_dataset_group.add_catch(
            notify_failed, errors=["ResourceFailed"], result_path="$.serviceError"
        )
        create_dataset_group.add_catch(
            notify_failed, errors=["States.ALL"], result_path="$.statesError"
        )

        create_glue_table_name = tasks.LambdaInvoke(
            self,
            "Create-Glue-Table-Name",
            lambda_function=self.functions["CreateGlueTableName"],
            result_path="$.glue_table_name",
            payload_response_only=True,
            retry_on_service_exceptions=True,
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
            backoff_rate=1.02,
            interval=Duration.seconds(120),
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

        create_predictor_backtest_export = tasks.LambdaInvoke(
            self,
            "Create-Predictor-Backtest-Export",
            lambda_function=self.functions["CreatePredictorBacktestExport"],
            result_path="$.PredictorArn",
            payload_response_only=True,
            retry_on_service_exceptions=True,
        )
        create_predictor_backtest_export.add_retry(
            backoff_rate=1.05,
            interval=Duration.seconds(5),
            max_attempts=100,
            errors=["ResourcePending"],
        )

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

        create_forecast_export = tasks.LambdaInvoke(
            self,
            "Create-Forecast-Export",
            lambda_function=self.functions["CreateForecastExport"],
            result_path="$.PredictorArn",
            payload_response_only=True,
            retry_on_service_exceptions=True,
        )
        create_forecast_export.add_retry(
            backoff_rate=1.05,
            interval=Duration.seconds(5),
            max_attempts=100,
            errors=["ResourcePending"],
        )

        create_quicksight_analysis = tasks.LambdaInvoke(
            self,
            "Create-QuickSight-Analysis",
            lambda_function=self.functions["CreateQuickSightAnalysis"],
            result_path=sfn.JsonPath.DISCARD,
            payload_response_only=True,
            retry_on_service_exceptions=True,
        )
        create_quicksight_analysis.add_catch(
            notify_prediction_failed,
            errors=["ResourceFailed"],
            result_path="$.serviceError",
        )
        create_quicksight_analysis.add_catch(
            notify_prediction_failed, errors=["States.ALL"], result_path="$.statesError"
        )

        forecast_etl = tasks.GlueStartJobRun(
            self,
            "Forecast-ETL",
            glue_job_name=f"{Aws.STACK_NAME}-Forecast-ETL",
            integration_pattern=sfn.IntegrationPattern.RUN_JOB,
            result_path=sfn.JsonPath.DISCARD,
            arguments=sfn.TaskInput.from_object(
                {
                    "--dataset_group": sfn.JsonPath.string_at("$.dataset_group_name"),
                    "--glue_table_name": sfn.JsonPath.string_at("$.glue_table_name"),
                }
            ),
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

        parallel_export = sfn.Parallel(
            self,
            "Export-Predictor-Backtest-And-Forecast",
            result_path=sfn.JsonPath.DISCARD,
        )
        parallel_export.branch(create_forecast_export)
        parallel_export.branch(create_predictor_backtest_export)
        parallel_export.add_catch(
            notify_prediction_failed,
            errors=["ResourceFailed"],
            result_path="$.serviceError",
        )
        parallel_export.add_catch(
            notify_prediction_failed, errors=["States.ALL"], result_path="$.statesError"
        )

        # step function definition
        definition = (
            check_error.when(sfn.Condition.is_present("$.serviceError"), notify_failed)
            .otherwise(create_dataset_group)  # temporary; for testing
            .afterwards()
            .next(import_data)
            .next(
                create_forecasts.iterator(
                    create_predictor.next(create_forecast)
                    .next(parallel_export)
                    .next(create_glue_table_name)
                    .next(forecast_etl)
                    .next(create_quicksight_analysis)
                    .next(notify_success)
                )
            )
        )

        self.state_machine = sfn.StateMachine(
            self,
            "DeployStateMachine",
            definition=definition,
            state_machine_name=f"Improving-Forecast-Accuracy-{Aws.STACK_NAME}",
        )
        add_cfn_nag_suppressions(
            resource=self.state_machine.role.node.children[1].node.default_child,
            suppressions=[
                CfnNagSuppression(
                    "W76",
                    "Large step functions need larger IAM roles to access all managed lambda functions",
                )
            ],
        )

    def set_forecast_permissions(self, name, data_bucket_name_resource: CfnResource):
        """All operations require access to read from S3"""
        function = self.functions[name]
        function.role.attach_inline_policy(
            self.policies.forecast_read_write_policy(name)
        )
        function.role.attach_inline_policy(
            self.policies.s3_bucket_read_policy(name, data_bucket_name_resource)
        )
        function.role.attach_inline_policy(self.policies.forecast_kms_read_policy)

    def set_s3_notification_permissions(self, data_bucket_name_resource: CfnResource):
        function_name = "S3NotificationLambda"
        function = self.functions[function_name]
        self.state_machine.grant_start_execution(function)
        function.add_environment(
            "STATE_MACHINE_ARN", self.state_machine.state_machine_arn
        )
        function.role.attach_inline_policy(
            self.policies.s3_bucket_read_policy(
                function_name, data_bucket_name_resource
            )
        )
        function.role.attach_inline_policy(self.policies.forecast_kms_read_policy)

    def set_forecast_s3_access_permissions(
        self,
        name,
        function: IFunction,
        data_bucket_name_resource: CfnResource,
        read: bool,
        write: bool,
    ):
        if read and write:
            policy = self.policies.forecast_s3_read_write_role
        elif read:
            policy = self.policies.forecast_s3_read_role
        else:
            raise ValueError("permissions must have read and write or just read access")

        forecast_s3_access_role = policy(
            name=name,
            data_bucket_name_resource=data_bucket_name_resource,
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
        function.add_environment("FORECAST_KMS", self.forecast_kms_key_arn)

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
        layer_deps = self.lambda_builder.layer_for(
            name="Dependencies",
            base="lambdas/lambda_dependencies",
            runtimes=[Runtime.PYTHON_3_8],
        )

        layer_data = self.lambda_builder.layer_for(
            name="DatasetUtils",
            base="lambdas/lambda_datasetutils",
            runtimes=[Runtime.PYTHON_3_8],
        )

        self.functions.update(
            self.lambda_builder.functions_for(
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
            self.lambda_builder.functions_for(
                name="CreateDatasetGroup",
                base="lambdas/createdatasetgroup",
                handlers="handler.createdatasetgroup",
                libs="shared",
                layers=[layer_deps],
            )
        )

        self.functions.update(
            self.lambda_builder.functions_for(
                name="CreateDatasetImportJob",
                base="lambdas/createdatasetimportjob",
                handlers="handler.createdatasetimportjob",
                libs="shared",
                layers=[layer_deps],
            )
        )

        self.functions.update(
            self.lambda_builder.functions_for(
                name="CreateForecast",
                base="lambdas/createforecast",
                handlers=[
                    "create_forecast.handler",
                    "create_forecast_export.handler",
                ],
                libs="shared",
                layers=[layer_deps],
            )
        )

        self.functions.update(
            self.lambda_builder.functions_for(
                name="CreatePredictor",
                base="lambdas/createpredictor",
                handlers=[
                    "create_predictor.handler",
                    "create_predictor_backtest_export.handler",
                ],
                libs="shared",
                layers=[layer_deps],
            )
        )

        self.functions.update(
            self.lambda_builder.functions_for(
                name="CreateQuickSightAnalysis",
                base="lambdas/createquicksightanalysis",
                handlers="handler.createquicksightanalysis",
                libs="shared",
                timeout=Duration.minutes(15),
                layers=[layer_data],
            )
        )

        self.functions.update(
            self.lambda_builder.functions_for(
                name="SNS",
                base="lambdas/sns",
                handlers="handler.sns",
                libs="shared",
                layers=[layer_deps],
            )
        )

        self.functions.update(
            self.lambda_builder.functions_for(
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

        self.functions.update(
            self.lambda_builder.functions_for(
                name="CreateGlueTableName",
                base="lambdas/creategluetablename",
                handlers="handler.creategluetablename",
                libs="shared",
                layers=[layer_deps],
            )
        )
