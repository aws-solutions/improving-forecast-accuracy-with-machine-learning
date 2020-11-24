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
    CfnParameter,
    App,
    CfnMapping,
    CfnCondition,
    Fn,
    Aspects,
    RemovalPolicy,
    Duration,
    CfnResource,
    Aws,
    CfnOutput,
)

from etl.athena import Athena
from etl.glue import Glue
from interfaces import ConditionalResources, TemplateOptions
from sagemaker.notebook import Notebook
from sns.notifications import Notifications
from solutions.cfn_nag import CfnNagSuppression, add_cfn_nag_suppressions
from solutions.metrics import Metrics
from stepfunctions.lambda_functions import LambdaFunctions


class ForecastStack(Stack):
    def __init__(self, app: App, id: str, **kwargs) -> None:
        super().__init__(app, id, **kwargs)

        self.template_options.description = "(SO0123) Improving Forecast Accuracy with Machine Learning %%VERSION%% - This solution provides a mechanism to automate Amazon Forecast predictor and forecast generation and visualize it via an Amazon SageMaker Jupyter Notebook"

        # set up the template parameters
        email = CfnParameter(
            self,
            id="Email",
            type="String",
            description="Email to notify with forecast results",
            default="",
            max_length=50,
            allowed_pattern=r"(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$|^$)",
            constraint_description="Must be a valid email address or blank",
        )

        lambda_log_level = CfnParameter(
            self,
            id="LambdaLogLevel",
            type="String",
            description="Change the verbosity of the logs output to CloudWatch",
            default="WARNING",
            allowed_values=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        )

        notebook_deploy = CfnParameter(
            self,
            id="NotebookDeploy",
            type="String",
            description="Deploy an Amazon SageMaker Jupyter Notebook instance",
            default="No",
            allowed_values=["Yes", "No"],
        )

        notebook_volume_size = CfnParameter(
            self,
            id="NotebookVolumeSize",
            type="Number",
            description="Enter the size of the notebook instance EBS volume in GB",
            default=10,
            min_value=5,
            max_value=16384,
            constraint_description="Must be an integer between 5 (GB) and 16384 (16 TB)",
        )

        notebook_instance_type = CfnParameter(
            self,
            id="NotebookInstanceType",
            type="String",
            description="Enter the type of the notebook instance",
            default="ml.t2.medium",
            allowed_values=[
                "ml.t2.medium",
                "ml.t3.medium",
                "ml.r5.large",
                "ml.c5.large",
            ],
        )

        quicksight_analysis_owner = CfnParameter(
            self,
            id="QuickSightAnalysisOwner",
            description="With QuickSight Enterprise enabled, provide a QuickSight ADMIN user ARN to automatically create QuickSight analyses",
            default="",
            allowed_pattern="(^arn:.*:quicksight:.*:.*:user.*$|^$)",
        )

        # set up the metadata/ cloudformation interface
        template_options = TemplateOptions()
        template_options.add_parameter_group(
            label="Improving Forecast Accuracy with Machine Learning Configuration",
            parameters=[email],
        )
        template_options.add_parameter_group(
            label="Visualization Options",
            parameters=[
                quicksight_analysis_owner,
                notebook_deploy,
                notebook_instance_type,
                notebook_volume_size,
            ],
        )
        template_options.add_parameter_group(
            label="Deployment Configuration", parameters=[lambda_log_level]
        )
        template_options.add_parameter_label(email, "Email")
        template_options.add_parameter_label(lambda_log_level, "CloudWatch Log Level")
        template_options.add_parameter_label(notebook_deploy, "Deploy Jupyter Notebook")
        template_options.add_parameter_label(
            notebook_volume_size, "Jupyter Notebook volume size"
        )
        template_options.add_parameter_label(
            notebook_instance_type, "Jupyter Notebook instance type"
        )
        template_options.add_parameter_label(
            quicksight_analysis_owner, "Deploy QuickSight Dashboards"
        )
        self.template_options.metadata = template_options.metadata

        solution_mapping = CfnMapping(
            self,
            "Solution",
            mapping={
                "Data": {
                    "ID": "SO0123",
                    "Version": "%%VERSION%%",
                    "SendAnonymousUsageData": "Yes",
                }
            },
        )

        source_mapping = CfnMapping(
            self,
            "SourceCode",
            mapping={
                "General": {
                    "S3Bucket": "%%BUCKET_NAME%%",
                    "KeyPrefix": "%%SOLUTION_NAME%%/%%VERSION%%",
                    "QuickSightSourceTemplateArn": "%%QUICKSIGHT_SOURCE%%",
                }
            },
        )

        # conditions
        create_notebook = CfnCondition(
            self,
            "CreateNotebook",
            expression=Fn.condition_equals(notebook_deploy, "Yes"),
        )
        email_provided = CfnCondition(
            self,
            "EmailProvided",
            expression=Fn.condition_not(Fn.condition_equals(email, "")),
        )
        send_anonymous_usage_data = CfnCondition(
            self,
            "SendAnonymousUsageData",
            expression=Fn.condition_equals(
                Fn.find_in_map("Solution", "Data", "SendAnonymousUsageData"), "Yes"
            ),
        )
        create_analysis = CfnCondition(
            self,
            "CreateAnalysis",
            expression=Fn.condition_not(
                Fn.condition_equals(quicksight_analysis_owner, ""),
            ),
        )

        # Step function and state machine
        fns = LambdaFunctions(self, "Functions", log_level=lambda_log_level)

        # SNS
        notifications = Notifications(
            self,
            "NotificationConfiguration",
            lambda_function=fns.functions["SNS"],
            email=email,
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
        glue = Glue(self, "GlueResources", unique_name)
        athena = Athena(self, "AthenaResources", athena_bucket=athena_bucket)

        # Configure permissions for functions
        fns.set_s3_notification_permissions(data_bucket_name_resource)
        fns.set_forecast_s3_access_permissions(
            name="DatasetImport",
            function=fns.functions["CreateDatasetImportJob"],
            data_bucket_name_resource=data_bucket_name_resource,
        )
        fns.set_forecast_s3_access_permissions(
            name="ForecastExport",
            function=fns.functions["CreateForecast"],
            data_bucket_name_resource=data_bucket_name_resource,
        )
        fns.set_forecast_etl_permissions(
            function=fns.functions["PrepareForecastExport"],
            database=glue.database,
            workgroup=athena.workgroup,
            quicksight_principal=quicksight_analysis_owner,
            quicksight_source=source_mapping,
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
            "PrepareForecastExport", data_bucket_name_resource=data_bucket_name_resource
        )

        # notebook (conditional on 'create_notebook')
        notebook = Notebook(
            self,
            "Notebook",
            buckets=[data_bucket],
            instance_type=notebook_instance_type.value_as_string,
            instance_volume_size=notebook_volume_size.value_as_number,
            notebook_path=Path(__file__).parent.parent.parent.joinpath(
                "notebook", "samples", "notebooks"
            ),
            notebook_destination_bucket=data_bucket,
            notebook_destination_prefix="notebooks",
        )
        Aspects.of(notebook).add(ConditionalResources(create_notebook))

        # solutions metrics (conditional on 'send_anonymous_usage_data')
        metrics = Metrics(
            self,
            "SolutionMetrics",
            metrics_function=fns.functions["CfnResourceSolutionMetrics"],
            metrics={
                "Solution": solution_mapping.find_in_map("Data", "ID"),
                "Version": solution_mapping.find_in_map("Data", "Version"),
                "Region": Aws.REGION,
                "NotebookDeployed": Fn.condition_if(
                    create_notebook.node.id, "Yes", "No"
                ),
                "NotebookType": Fn.condition_if(
                    create_notebook.node.id,
                    notebook_instance_type.value_as_string,
                    Aws.NO_VALUE,
                ),
                "QuickSightDeployed": Fn.condition_if(
                    create_analysis.node.id, "Yes", "No"
                ),
            },
        )
        Aspects.of(metrics).add(ConditionalResources(send_anonymous_usage_data))

        # outputs
        CfnOutput(self, "ForecastBucketName", value=data_bucket.bucket_name)
        CfnOutput(self, "AthenaBucketName", value=athena_bucket.bucket_name)
        CfnOutput(self, "StepFunctionsName", value=fns.state_machine.state_machine_name)

    def secure_bucket(self, name, suppressions=None, **kwargs):
        bucket = Bucket(
            self,
            name,
            removal_policy=RemovalPolicy.RETAIN,
            encryption=BucketEncryption.S3_MANAGED,
            block_public_access=BlockPublicAccess.BLOCK_ALL,
            **kwargs
        )
        bucket.add_to_resource_policy(
            iam.PolicyStatement(
                sid="HttpsOnly",
                resources=[bucket.arn_for_objects("*"),],
                actions=["*"],
                effect=iam.Effect.DENY,
                principals=[iam.AnyPrincipal()],
                conditions={"Bool": {"aws:SecureTransport": False}},
            )
        )
        bucket_cfn = bucket.node.default_child  # type: CfnResource
        bucket_cfn.override_logical_id(name)
        if suppressions:
            add_cfn_nag_suppressions(bucket_cfn, suppressions)

        return bucket
