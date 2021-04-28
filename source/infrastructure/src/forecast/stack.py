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

import aws_cdk.aws_iam as iam
from aws_cdk.aws_s3 import (
    Bucket,
    BucketEncryption,
    LifecycleRule,
    BlockPublicAccess,
    BucketAccessControl,
)
from aws_cdk.core import (
    Stack,
    Construct,
    CfnCondition,
    Fn,
    Aspects,
    RemovalPolicy,
    Duration,
    CfnResource,
    Aws,
    CfnOutput,
    NestedStack,
)

from etl.athena import Athena
from etl.glue import Glue
from forecast.parameters import Parameters
from interfaces import ConditionalResources, TemplateOptions
from interfaces.CfnNagSuppressAll import CfnNagSuppressAll
from sagemaker.notebook import Notebook
from sns.notifications import Notifications
from solutions.cfn_nag import CfnNagSuppression, add_cfn_nag_suppressions
from solutions.mappings import Mappings
from solutions.metrics import Metrics
from stepfunctions.lambda_functions import LambdaFunctions


class ForecastStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        self.mappings = Mappings(self, "SO0123", quicksight_template_arn=True)
        self.parameters = Parameters(self)
        template_options = TemplateOptions(
            self,
            id=id,
            description="(SO0123) Improving Forecast Accuracy with Machine Learning %%VERSION%% - This solution provides a mechanism to automate Amazon Forecast predictor and forecast generation and visualize it via Amazon Quicksight or an Amazon SageMaker Jupyter Notebook",
            filename="improving-forecast-accuracy-with-machine-learning.template",
        )
        self.parameters.add_template_options(template_options)

        # conditions
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
        forecast_kms = CfnCondition(
            self,
            "ForecastSseKmsEnabled",
            expression=Fn.condition_not(
                Fn.condition_equals(self.parameters.forecast_kms_key_arn, "")
            ),
        )

        # Step function and state machine
        fns = LambdaFunctions(
            self,
            "Functions",
            log_level=self.parameters.lambda_log_level,
            forecast_kms=forecast_kms,
            forecast_kms_key_arn=self.parameters.forecast_kms_key_arn.value_as_string,
        )

        # SNS
        self.notifications = Notifications(
            self,
            "NotificationConfiguration",
            lambda_function=fns.functions["SNS"],
            email=self.parameters.email,
            email_provided=email_provided,
        )

        # Custom Resources
        unique_name = CfnResource(
            self,
            "UniqueName",
            type="Custom::UniqueName",
            properties={
                "ServiceToken": fns.functions["CfnResourceUniqueName"].function_arn
            },
        )
        unique_name.override_logical_id("UniqueName")

        data_bucket_name_resource = CfnResource(
            self,
            "DataBucketName",
            type="Custom::BucketName",
            properties={
                "ServiceToken": fns.functions["CfnResourceBucketName"].function_arn,
                "BucketPurpose": "data-bucket",
                "StackName": Aws.STACK_NAME,
                "Id": unique_name.get_att("Id"),
            },
        )
        data_bucket_name_resource.override_logical_id("DataBucketName")

        # Buckets
        access_logs_bucket = self.secure_bucket(
            "AccessLogsBucket",
            suppressions=[
                CfnNagSuppression(
                    "W35",
                    "This bucket is used as the logging destination for forecast datasets and exports",
                )
            ],
            access_control=BucketAccessControl.LOG_DELIVERY_WRITE,
        )

        athena_bucket = self.secure_bucket(
            "AthenaBucket",
            server_access_logs_bucket=access_logs_bucket,
            server_access_logs_prefix="athena-bucket-access-logs/",
        )

        data_bucket = self.secure_bucket(
            "ForecastBucket",
            lifecycle_rules=[
                LifecycleRule(
                    abort_incomplete_multipart_upload_after=Duration.days(3),
                    enabled=True,
                ),
                LifecycleRule(expiration=Duration.days(1), prefix="raw/", enabled=True),
            ],
            bucket_name=data_bucket_name_resource.get_att("Name").to_string(),
            server_access_logs_bucket=access_logs_bucket,
            server_access_logs_prefix="forecast-bucket-access-logs/",
        )
        data_bucket.node.default_child.add_property_override(
            "NotificationConfiguration",
            {
                "LambdaConfigurations": [
                    {
                        "Function": fns.functions["S3NotificationLambda"].function_arn,
                        "Event": "s3:ObjectCreated:*",
                        "Filter": {
                            "S3Key": {
                                "Rules": [
                                    {"Name": "prefix", "Value": "train/"},
                                    {"Name": "suffix", "Value": ".csv"},
                                ]
                            }
                        },
                    }
                ]
            },
        )

        # Glue and Athena
        glue = Glue(
            self,
            "GlueResources",
            unique_name,
            forecast_bucket=data_bucket,
            athena_bucket=athena_bucket,
            glue_jobs_path=Path(__file__).parent.parent.parent.parent.joinpath(
                "glue", "jobs"
            ),
        )
        athena = Athena(self, "AthenaResources", athena_bucket=athena_bucket)

        # Configure permissions for functions
        fns.set_s3_notification_permissions(data_bucket_name_resource)
        fns.set_forecast_s3_access_permissions(
            name="CreateDatasetGroup",
            function=fns.functions["CreateDatasetGroup"],
            data_bucket_name_resource=data_bucket_name_resource,
            read=True,
            write=True,
        )
        fns.set_forecast_s3_access_permissions(
            name="DatasetImport",
            function=fns.functions["CreateDatasetImportJob"],
            data_bucket_name_resource=data_bucket_name_resource,
            read=True,
            write=False,
        )
        fns.set_forecast_s3_access_permissions(
            name="CreatePredictor",
            function=fns.functions["CreatePredictor"],
            data_bucket_name_resource=data_bucket_name_resource,
            read=True,
            write=True,
        )
        fns.set_forecast_s3_access_permissions(
            name="ForecastExport",
            function=fns.functions["CreateForecastExport"],
            data_bucket_name_resource=data_bucket_name_resource,
            read=True,
            write=True,
        )
        fns.set_forecast_s3_access_permissions(
            name="PredictorBacktestExport",
            function=fns.functions["CreatePredictorBacktestExport"],
            data_bucket_name_resource=data_bucket_name_resource,
            read=True,
            write=True,
        )
        fns.set_forecast_etl_permissions(
            function=fns.functions["CreateQuickSightAnalysis"],
            database=glue.database,
            workgroup=athena.workgroup,
            quicksight_principal=self.parameters.quicksight_analysis_owner,
            quicksight_source=self.mappings.source_mapping,
            athena_bucket=athena_bucket,
            data_bucket_name_resource=data_bucket_name_resource,
        )
        fns.set_forecast_permissions(
            "CreateDatasetGroup", data_bucket_name_resource=data_bucket_name_resource
        )
        fns.set_forecast_permissions(
            "CreateDatasetImportJob",
            data_bucket_name_resource=data_bucket_name_resource,
        )
        fns.set_forecast_permissions(
            "CreateForecast", data_bucket_name_resource=data_bucket_name_resource
        )
        fns.set_forecast_permissions(
            "CreatePredictor", data_bucket_name_resource=data_bucket_name_resource
        )
        fns.set_forecast_permissions(
            "CreateQuickSightAnalysis",
            data_bucket_name_resource=data_bucket_name_resource,
        )

        # notebook (conditional on 'create_notebook')
        notebook = Notebook(
            self,
            "Notebook",
            buckets=[data_bucket],
            instance_type=self.parameters.notebook_instance_type.value_as_string,
            instance_volume_size=self.parameters.notebook_volume_size.value_as_number,
            notebook_path=Path(__file__).parent.parent.parent.parent.joinpath(
                "notebook", "samples", "notebooks"
            ),
            notebook_destination_bucket=data_bucket,
            notebook_destination_prefix="notebooks",
        )
        Aspects.of(notebook).add(ConditionalResources(create_notebook))

        # solutions metrics (conditional on 'send_anonymous_usage_data')
        self.metrics = Metrics(
            self,
            "SolutionMetrics",
            metrics_function=fns.functions["CfnResourceSolutionMetrics"],
            metrics={
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
            },
        )

        # aspects
        Aspects.of(self).add(
            CfnNagSuppressAll(
                suppress=[
                    CfnNagSuppression(
                        "W89", "Solution AWS Lambda Functions are not deployed to a VPC"
                    ),
                    CfnNagSuppression(
                        "W92",
                        "Solution AWS Lambda Functions do not require reserved concurrency",
                    ),
                ],
                resource_type="AWS::Lambda::Function",
            )
        )

        # outputs
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
            value=fns.state_machine.state_machine_name,
            export_name=f"{Aws.STACK_NAME}-StepFunctionsName",
        )

    def secure_bucket(self, name, suppressions=None, **kwargs):
        bucket = Bucket(
            self,
            name,
            removal_policy=RemovalPolicy.RETAIN,
            encryption=BucketEncryption.S3_MANAGED,
            block_public_access=BlockPublicAccess.BLOCK_ALL,
            **kwargs,
        )
        bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="HttpsOnly",
                resources=[
                    bucket.arn_for_objects("*"),
                ],
                actions=["*"],
                effect=iam.Effect.DENY,
                principals=[iam.AnyPrincipal()],
                conditions={"Bool": {"aws:SecureTransport": False}},
            )
        )
        bucket_cfn = bucket.node.default_child  # type: CfnResource
        bucket_cfn.add_property_override(
            "BucketEncryption",
            {
                "ServerSideEncryptionConfiguration": [
                    {
                        "BucketKeyEnabled": True,
                        "ServerSideEncryptionByDefault": {
                            "SSEAlgorithm": "AES256",
                        },
                    }
                ]
            },
        )
        bucket_cfn.override_logical_id(name)
        if suppressions:
            add_cfn_nag_suppressions(bucket_cfn, suppressions)

        return bucket


class NestedForecastStack(ForecastStack, NestedStack):
    """The nested version of the forecast stack to use in the demo"""

    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
