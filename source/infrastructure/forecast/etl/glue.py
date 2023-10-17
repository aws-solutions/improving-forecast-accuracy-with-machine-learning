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
from constructs import Construct
from aws_cdk.aws_s3 import IBucket, Location
from aws_cdk.aws_s3_deployment import BucketDeployment, Source
from aws_cdk import Aws, CfnResource, Fn

from aws_solutions.cdk.aws_lambda.cfn_custom_resources.url_downloader import (
    UrlDownloader,
)
from aws_solutions.cdk.cfn_nag import add_cfn_nag_suppressions, CfnNagSuppression
from aws_solutions.cdk.utils import is_solution_build
from forecast.etl.policies import GluePolicies


class Glue(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        unique_name: CfnResource,
        forecast_bucket: IBucket,
        athena_bucket: IBucket,
        glue_jobs_path: Path,
    ):
        super().__init__(scope, id)

        self.policies = GluePolicies()

        # implementation of CDK CfnDatabase is incomplete, use CfnResource
        self.database = CfnResource(
            self,
            "DataCatalog",
            type="AWS::Glue::Database",
            properties={
                "CatalogId": Aws.ACCOUNT_ID,
                "DatabaseInput": {
                    "Name": f"forecast_{unique_name}",
                    "Description": f"Database for Improving Forecast Accuracy with Machine Learning (stack: {Aws.STACK_NAME})",
                },
            },
        )
        self.database.override_logical_id("DataCatalog")

        self.glue_role = iam.Role(
            self,
            "GlueRole",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            inline_policies={
                "S3SolutionAccess": self.policies.s3_read_write_access(
                    [athena_bucket, forecast_bucket]
                ),
                "S3StackAccess": self.policies.s3_solutions_read_access(),
                "ForecastRead": self.policies.forecast_read(),
                "CloudwatchLogsAccess": self.policies.cloudwatch_logs_write(),
                "GlueAccess": self.policies.glue_access(
                    database=self.database,
                    athena_bucket=athena_bucket,
                    data_bucket=forecast_bucket,
                ),
            },
        )
        add_cfn_nag_suppressions(
            self.glue_role.node.default_child,
            [
                CfnNagSuppression(
                    "W11",
                    "Require access to all resources; Not all Amazon Forecast resources support resource based policy",
                )
            ],
        )

        # deploy the glue script locally or from the solutions bucket
        if is_solution_build(self):
            # deploy the asset (from the solutions bucket)
            self.downloader = UrlDownloader(
                self,
                "GlueJob",
                source=Location(
                    bucket_name=f"{Fn.find_in_map('SourceCode', 'General', 'S3Bucket')}-{Aws.REGION}",
                    object_key=f"{Fn.find_in_map('SourceCode', 'General', 'KeyPrefix')}/glue/jobs/forecast_etl.py",
                ),
                destination=Location(
                    bucket_name=forecast_bucket.bucket_name,
                    object_key="glue/forecast_etl.py",
                ),
            )
        else:
            # deploy the asset (from CDK assets)
            sources = [Source.asset(path=str(glue_jobs_path))]
            self.glue_script_deployment = BucketDeployment(
                self,
                "GlueJob",
                destination_bucket=forecast_bucket,
                destination_key_prefix="glue",
                sources=sources,
            )

        self.glue_job = CfnResource(
            self,
            "ForecastETLJob",
            type="AWS::Glue::Job",
            properties={
                "Name": f"{Aws.STACK_NAME}-Forecast-ETL",
                "Description": "Improving Forecast Accuracy with Machine Learning - Glue Job",
                "Role": self.glue_role.role_arn,
                "Command": {
                    "Name": "glueetl",
                    "PythonVersion": "3",
                    "ScriptLocation": forecast_bucket.s3_url_for_object(
                        "glue/forecast_etl.py"
                    ),
                },
                "DefaultArguments": {
                    "--region": Aws.REGION,
                    "--account_id": Aws.ACCOUNT_ID,
                    "--database": self.database.ref,
                    "--data_bucket": forecast_bucket.bucket_name,
                    "--job-bookmark-option": "job-bookmark-disable",
                    "--job-language": "python",
                    "--encryption-type": "sse-s3",
                    "--additional-python-modules": "boto3==1.26.83",
                },
                "GlueVersion": "2.0",
                "WorkerType": "G.1X",
                "NumberOfWorkers": 2,
                "ExecutionProperty": {
                    "MaxConcurrentRuns": 5,
                },
            },
        )
