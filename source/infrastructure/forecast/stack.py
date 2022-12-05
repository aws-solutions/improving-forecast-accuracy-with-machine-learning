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

from aws_cdk.aws_s3 import EventType, NotificationKeyFilter
from aws_cdk.aws_s3_notifications import LambdaDestination
from aws_cdk.aws_stepfunctions import (
    StateMachine,
    Fail,
    Parallel,
    Chain,
    Map,
    JsonPath,
    Succeed,
    IntegrationPattern,
    TaskInput,
    Choice,
    Condition,
    Pass,
)
from aws_cdk.aws_stepfunctions_tasks import GlueStartJobRun
from aws_cdk.core import (
    Construct,
    Fn,
    CfnCondition,
    Tags,
    Aspects,
    Duration,
    CfnOutput,
    Aws,
)

from aws_solutions.cdk.aspects import ConditionalResources
from aws_solutions.cdk.aws_lambda.cfn_custom_resources.resource_name import ResourceName
from aws_solutions.cdk.aws_lambda.cfn_custom_resources.url_helper import UrlHelper
from aws_solutions.cdk.cfn_nag import (
    CfnNagSuppressAll,
    CfnNagSuppression,
    add_cfn_nag_suppressions,
)
from aws_solutions.cdk.stack import SolutionStack
from forecast.aws_lambda.functions import (
    S3EventHandler,
    CreateDatasetGroup,
    CreateDatasetImportJob,
    CreatePredictor,
    CreateForecast,
    CreateForecastExport,
    CreatePredictorBacktestExport,
    Notifications,
    CreateGlueTableName,
    CreateQuickSightAnalysis,
)
from forecast.aws_lambda.layers import ForecastSolutionLayer
from forecast.aws_lambda.policies.factory import PolicyFactory
from forecast.buckets import (
    DataBucket,
    AthenaBucket,
    AccessLogsBucket,
)
from forecast.etl import Glue, Athena
from forecast.forecast.downloader import Downloader
from forecast.forecast.parameters import Parameters
from forecast.sagemaker.notebook import Notebook


class ForecastStack(SolutionStack):
    def __init__(self, scope: Construct, construct_id: str, *args, **kwargs) -> None:
        super().__init__(scope, construct_id, *args, **kwargs)

        # Parameters
        self.parameters = Parameters(self, "ForecastStackParameters")

        # Conditions
        create_notebook = CfnCondition(
            self,
            "CreateNotebook",
            expression=Fn.condition_equals(self.parameters.notebook_deploy, "Yes"),
        )
        email_provided = CfnCondition(
            self,
            "EmailProvided",
            expression=Fn.condition_not(Fn.condition_equals(self.parameters.email, "")),
        )
        create_analysis = CfnCondition(
            self,
            "CreateAnalysis",
            expression=Fn.condition_not(
                Fn.condition_equals(self.parameters.quicksight_analysis_owner, ""),
            ),
        )
        forecast_kms_enabled = CfnCondition(
            self,
            "ForecastSseKmsEnabled",
            expression=Fn.condition_not(
                Fn.condition_equals(self.parameters.forecast_kms_key_arn, "")
            ),
        )
        create_forecast_cdn = CfnCondition(
            self,
            "CreateForecast",
            expression=Fn.condition_equals(self.parameters.forecast_deploy, "Yes"),
        )

        # Buckets
        data_bucket_name_resource = ResourceName(
            self,
            "DataBucketName",
            purpose="data-bucket",
            max_length=63,
        )

        access_logs_bucket = AccessLogsBucket(self)

        athena_bucket = AthenaBucket(
            self,
            server_access_logs_bucket=access_logs_bucket,
            server_access_logs_prefix="athena-bucket-access-logs/",
        )

        data_bucket = DataBucket(
            self,
            bucket_name=data_bucket_name_resource.resource_name.to_string(),
            server_access_logs_bucket=access_logs_bucket,
            server_access_logs_prefix="forecast-bucket-access-logs/",
        )

        policy_factory = PolicyFactory(
            self,
            "ForecastPolicyFactory",
            data_bucket=data_bucket,
            kms_key_arn=self.parameters.forecast_kms_key_arn.value_as_string,
            kms_enabled=forecast_kms_enabled,
        )

        # Lambda Functions
        default_timeout = Duration.minutes(3)
        solution_layer = ForecastSolutionLayer(self, "SolutionLayer")
        create_dataset_group = CreateDatasetGroup(
            self, "CreateDatasetGroup", layers=[solution_layer], timeout=default_timeout
        )
        create_dataset_import_job = CreateDatasetImportJob(
            self,
            "CreateDatasetImportJob",
            layers=[solution_layer],
            timeout=default_timeout,
        )
        create_predictor = CreatePredictor(
            self, "CreatePredictor", layers=[solution_layer], timeout=default_timeout
        )
        create_forecast = CreateForecast(
            self, "CreateForecast", layers=[solution_layer], timeout=default_timeout
        )
        create_forecast_export = CreateForecastExport(
            self,
            "CreateForecastExport",
            layers=[solution_layer],
            timeout=default_timeout,
        )
        create_predictor_backtest_export = CreatePredictorBacktestExport(
            self,
            "CreatePredictorBacktestExport",
            layers=[solution_layer],
            timeout=default_timeout,
        )
        create_glue_table_name = CreateGlueTableName(
            self, "CreateGlueTableName", layers=[solution_layer]
        )
        create_quicksight_analysis = CreateQuickSightAnalysis(
            self,
            "CreateQuickSightAnalysis",
            layers=[solution_layer],
            timeout=Duration.minutes(15),
        )

        notifications = Notifications(
            self,
            "SNS Notification",
            email=self.parameters.email,
            email_provided=email_provided,
            layers=[solution_layer],
        )

        # State Machine
        check_error = Choice(self, "Check for Error")
        notify_failure = notifications.state(
            self, "Notify on Failure", result_path=JsonPath.DISCARD
        )
        notify_success = notifications.state(
            self, "Notify on Success", result_path=JsonPath.DISCARD
        )
        create_predictor_state = create_predictor.state(
            self,
            "Create Predictor",
            result_path="$.PredictorArn",  # NOSONAR (python:S1192) - string for clarity
            max_attempts=100,
            interval=Duration.seconds(120),
            backoff_rate=1.02,
        )
        create_predictor_state.start_state.add_catch(
            Succeed(self, "Update Not Required"), errors=["NotMostRecentUpdate"]
        )
        create_predictor_state.start_state.add_retry(
            backoff_rate=1.02,
            interval=Duration.seconds(120),
            max_attempts=100,
            errors=["DatasetsImporting"],
        )
        forecast_etl = GlueStartJobRun(
            self,
            "Forecast ETL",
            glue_job_name=f"{Aws.STACK_NAME}-Forecast-ETL",
            integration_pattern=IntegrationPattern.RUN_JOB,
            result_path=JsonPath.DISCARD,
            arguments=TaskInput.from_object(
                {
                    "--dataset_group": JsonPath.string_at("$.dataset_group_name"),
                    "--glue_table_name": JsonPath.string_at("$.glue_table_name"),
                }
            ),
        )
        forecast_etl.add_retry(
            backoff_rate=1.02,
            interval=Duration.seconds(120),
            max_attempts=100,
            errors=["Glue.ConcurrentRunsExceededException"],
        )

        definition = Chain.start(
            check_error.when(
                Condition.is_present("$.error.serviceError"), notify_failure
            ).otherwise(
                Parallel(self, "Manage the Execution")
                .branch(
                    create_dataset_group.state(
                        self,
                        "Create Dataset Group",
                        result_path="$.DatasetGroupNames",
                    )
                    .next(
                        create_dataset_import_job.state(
                            self,
                            "Create Dataset Import Job",
                            result_path="$.DatasetImportJobArn",
                            max_attempts=100,
                            interval=Duration.seconds(120),
                            backoff_rate=1.02,
                        )
                    )
                    .next(
                        Map(
                            self,
                            "Create Forecasts",
                            items_path="$.DatasetGroupNames",
                            parameters={
                                "bucket.$": "$.bucket",
                                "dataset_file.$": "$.dataset_file",
                                "dataset_group_name.$": "$$.Map.Item.Value",
                                "config.$": "$.config",
                            },
                        ).iterator(
                            create_predictor_state.next(
                                create_forecast.state(
                                    self,
                                    "Create Forecast",
                                    result_path="$.ForecastArn",
                                    max_attempts=100,
                                )
                            ).next(
                                Parallel(
                                    self,
                                    "Export Predictor Backtest and Forecast",
                                    result_path=JsonPath.DISCARD,
                                )
                                .branch(
                                    create_forecast_export.state(
                                        self,
                                        "Create Forecast Export",
                                        result_path="$.PredictorArn",  # NOSONAR (python:S1192) - string for clarity
                                        max_attempts=100,
                                    )
                                )
                                .branch(
                                    create_predictor_backtest_export.state(
                                        self,
                                        "Create Predictor Backtest Export",
                                        result_path="$.PredictorArn",  # NOSONAR (python:S1192) - string for clarity
                                        max_attempts=100,
                                    )
                                )
                                .next(
                                    create_glue_table_name.state(
                                        self,
                                        "Create Glue Table Name",
                                        result_path="$.glue_table_name",
                                    )
                                )
                                .next(forecast_etl)
                                .next(
                                    create_quicksight_analysis.state(
                                        self,
                                        "Create QuickSight Analysis",
                                        result_path=JsonPath.DISCARD,
                                    )
                                )
                                .next(notify_success)
                            )
                        )
                    )
                )
                .add_catch(
                    notify_failure.next(Fail(self, "Failure")), result_path="$.error"
                )
            )
        )
        # fmt: on

        state_machine_namer = ResourceName(
            self, "StateMachineName", purpose="forecast-workflow", max_length=80
        )
        state_machine = StateMachine(
            self,
            "ForecastStateMachine",
            state_machine_name=state_machine_namer.resource_name.to_string(),
            definition=definition,
            tracing_enabled=True,
        )
        add_cfn_nag_suppressions(
            resource=state_machine.role.node.children[1].node.default_child,
            suppressions=[
                CfnNagSuppression(
                    "W76",
                    "Large step functions need larger IAM roles to access all managed lambda functions",
                ),
                CfnNagSuppression(
                    "W12", "IAM policy for AWS X-Ray requires an allow on *"
                ),
            ],
        )

        # S3 Notifications
        s3_event_handler = S3EventHandler(
            self,
            "S3EventHandler",
            state_machine=state_machine,
            bucket=data_bucket,
            layers=[solution_layer],
            timeout=Duration.minutes(1),
        )
        s3_event_notification = LambdaDestination(s3_event_handler)
        data_bucket.add_event_notification(
            EventType.OBJECT_CREATED,
            s3_event_notification,
            NotificationKeyFilter(prefix="train/", suffix=".csv"),
        )

        # Handle suppressions for the notification handler resource generated by CDK
        bucket_notification_handler = self.node.try_find_child(
            "BucketNotificationsHandler050a0587b7544547bf325f094a3db834"
        )
        bucket_notification_policy = (
            bucket_notification_handler.node.find_child("Role")
            .node.find_child("DefaultPolicy")
            .node.find_child("Resource")
        )
        add_cfn_nag_suppressions(
            bucket_notification_policy,
            [
                CfnNagSuppression(
                    "W12",
                    "bucket resource is '*' due to circular dependency with bucket and role creation at the same time",
                )
            ],
        )

        # ETL Components
        glue = Glue(
            self,
            "GlueResources",
            unique_name=data_bucket_name_resource.resource_id.to_string(),
            forecast_bucket=data_bucket,
            athena_bucket=athena_bucket,
            glue_jobs_path=Path(__file__).parents[2] / "glue" / "jobs",
        )
        athena = Athena(self, "AthenaResources", athena_bucket=athena_bucket)

        # Permissions
        policy_factory.grant_forecast_read_write(create_dataset_group.function)
        policy_factory.grant_forecast_read_write(create_dataset_import_job.function)
        policy_factory.grant_forecast_read_write(create_predictor.function)
        policy_factory.grant_forecast_read_write(
            create_predictor_backtest_export.function
        )
        policy_factory.grant_forecast_read(create_forecast.function)
        policy_factory.grant_forecast_read_write(create_forecast_export.function)
        policy_factory.grant_forecast_read(create_quicksight_analysis.function)
        policy_factory.quicksight_access(
            create_quicksight_analysis.function,
            catalog=glue.database,
            workgroup=athena.workgroup,
            quicksight_principal=self.parameters.quicksight_analysis_owner,
            quicksight_source=self.mappings.source_mapping,
            athena_bucket=athena_bucket,
            data_bucket=data_bucket,
        )
        data_bucket.grant_read(create_dataset_group.function)
        data_bucket.grant_read(create_dataset_import_job.function)
        data_bucket.grant_read(create_predictor.function)
        data_bucket.grant_read_write(create_predictor_backtest_export.function)
        data_bucket.grant_read(create_forecast.function)
        data_bucket.grant_read_write(create_forecast_export.function)
        data_bucket.grant_read(s3_event_handler)

        # Notebook
        Notebook(
            self,
            "Notebook",
            buckets=[data_bucket],
            instance_type=self.parameters.notebook_instance_type.value_as_string,
            instance_volume_size=self.parameters.notebook_volume_size.value_as_number,
            notebook_path=Path(__file__).parents[2]
            / "notebook"
            / "samples"
            / "notebooks",
            notebook_destination_bucket=data_bucket,
            notebook_destination_prefix="notebooks",
            create_notebook=create_notebook,
        )

        # Demo components
        self.forecast_defaults_url_info = UrlHelper(
            self,
            "ForecastDefaults",
            self.parameters.forecast_defaults_url.value_as_string,
        )
        self.tts_url_info = UrlHelper(
            self, "TTS", self.parameters.tts_url.value_as_string
        )
        self.rts_url_info = UrlHelper(
            self, "RTS", self.parameters.rts_url.value_as_string
        )
        self.md_url_info = UrlHelper(self, "MD", self.parameters.md_url.value_as_string)

        # prepare the nested stack that performs the actual downloads
        downloader = Downloader(
            self,
            "Downloader",
            description="Improving Forecast Accuracy with Machine Learning Data copier",
            template_filename="improving-forecast-accuracy-with-machine-learning-downloader.template",
            parameters={
                "DestinationBucket": data_bucket.bucket_name,
                "ForecastName": self.parameters.forecast_name.value_as_string,
                "Version": self.node.try_get_context("SOLUTION_VERSION"),
                **self.forecast_defaults_url_info.properties,
                **self.tts_url_info.properties,
                **self.rts_url_info.properties,
                **self.md_url_info.properties,
            },
        )
        downloader.nested_stack_resource.override_logical_id("Downloader")
        downloader.node.default_child.cfn_options.condition = create_forecast_cdn


        # Tagging
        Tags.of(self).add("SOLUTION_ID", self.node.try_get_context("SOLUTION_ID"))
        Tags.of(self).add("SOLUTION_NAME", self.node.try_get_context("SOLUTION_NAME"))
        Tags.of(self).add(
            "SOLUTION_VERSION", self.node.try_get_context("SOLUTION_VERSION")
        )

        # Aspects
        Aspects.of(self).add(
            CfnNagSuppressAll(
                suppress=[
                    CfnNagSuppression(
                        "W89",
                        "functions deployed by this solution do not require VPC access",
                    ),
                    CfnNagSuppression(
                        "W92",
                        "functions deployed by this solution do not require reserved concurrency",
                    ),
                    CfnNagSuppression(
                        "W58",
                        "functions deployed by this solution use custom policy to write to CloudWatch logs",
                    ),
                ],
                resource_type="AWS::Lambda::Function",
            )
        )

        # Metrics
        self.metrics.update(
            {
                "Solution": self.mappings.solution_mapping.find_in_map("Data", "ID"),
                "Version": self.mappings.solution_mapping.find_in_map(
                    "Data", "Version"
                ),
                "Region": Aws.REGION,
                "NotebookDeployed": Fn.condition_if(
                    create_notebook.node.id, "Yes", "No"
                ),
                "NotebookType": Fn.condition_if(
                    create_notebook.node.id,
                    self.parameters.notebook_instance_type.value_as_string,
                    Aws.NO_VALUE,
                ),
                "QuickSightDeployed": Fn.condition_if(
                    create_analysis.node.id, "Yes", "No"
                ),
                "ForecastDeployed": Fn.condition_if(
                    create_forecast_cdn.node.id, "Yes", "No"
                ),
            }
        )

        # Outputs
        CfnOutput(
            self,
            "ForecastBucketName",
            value=data_bucket.bucket_name,
            export_name=f"{Aws.STACK_NAME}-ForecastBucketName",
        )
        CfnOutput(
            self,
            "AthenaBucketName",
            value=athena_bucket.bucket_name,
            export_name=f"{Aws.STACK_NAME}-AthenaBucketName",
        )
        CfnOutput(
            self,
            "StepFunctionsName",
            value=state_machine.state_machine_name,
            export_name=f"{Aws.STACK_NAME}-StepFunctionsName",
        )
