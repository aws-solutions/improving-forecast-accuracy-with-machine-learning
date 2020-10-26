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

import os

import boto3
import pytest
import yaml
from moto import mock_s3

CONFIG_FILE = "config_and_overrides.yaml"


@pytest.fixture(autouse=True)
def aws_credentials():
    """Mocked AWS Credentials"""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_REGION"] = "us-east-1"  # must be a valid region
    os.environ["FORECAST_ROLE"] = "arn:aws:iam::abcdefghijkl:role/some-forecast-role"
    os.environ[
        "SNS_TOPIC_ARN"
    ] = "arn:aws:sns:us-east-1:abcdefghijkl:some-forecast-notification-topic"
    os.environ["EXPORT_ROLE"] = "arn:aws:iam::abcdefghijkl:role/export-role"
    os.environ[
        "STEP_FUNCTIONS_ARN"
    ] = "DeployStateMachine	arn:aws:states:us-east-1:abcdefghijkl:stateMachine:Forecast-Workflow-Automation-forecast-unsamez"


@pytest.fixture(scope="function")
def s3_valid_config():
    with mock_s3():
        with open(
            os.path.join(os.path.dirname(__file__), "fixtures", CONFIG_FILE), "r",
        ) as f:
            body = f.read()

        client = boto3.client("s3", region_name=os.environ.get("AWS_REGION"))
        client.create_bucket(Bucket="testbucket")
        client.put_object(Bucket="testbucket", Key="forecast-defaults.yaml", Body=body)
        yield client


@pytest.fixture(scope="function")
def s3_valid_files():
    demand_file = os.path.join(
        os.path.dirname(__file__), os.path.pardir, "example", "data", "demand.csv"
    )
    related_file = os.path.join(
        os.path.dirname(__file__),
        os.path.pardir,
        "example",
        "data",
        "demand.related.csv",
    )
    metadata_file = os.path.join(
        os.path.dirname(__file__),
        os.path.pardir,
        "example",
        "data",
        "demand.metadata.csv",
    )

    bucket = "testbucket"

    with mock_s3():
        client = boto3.client("s3", region_name=os.environ.get("AWS_REGION"))
        client.create_bucket(Bucket=bucket)

        for path in [demand_file, related_file, metadata_file]:
            with open(path, "r") as f:
                body = f.read()
            for item in "T,TR,TM,TRM".split(","):
                client.put_object(
                    Bucket=bucket,
                    Key=f"train/{path.split('/')[-1]}".replace(
                        "demand", f"RetailDemand{item}"
                    ),
                    Body=body,
                )

        # simulate an export object as well
        export_body = "item_id,date,location,p10,p50,p90\n"
        export_body += "alfredo y,1999-12-31T01:00:00Z,kanata,1.1,3.3,5.5"
        export_body += "alfredo y,1999-12-31T02:00:00Z,kanata,1.5,3.4,5.4"
        client.put_object(
            Bucket=bucket,
            Key=f"exports/export_RetailDemandTRM_2000_01_01_00_00_00/some_file.csv",
            Body=export_body,
        )
        client.put_object(
            Bucket=bucket,
            Key=f"exports/export_RetailDemandTRM_2000_01_01_00_00_00/empty.csv",
            Body="",
        )

        yield client


@pytest.fixture(scope="function")
def s3_missing_config():
    with mock_s3():
        client = boto3.client("s3", region_name=os.environ.get("AWS_REGION"))
        client.create_bucket(Bucket="testbucket")
        yield client


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_s3():
        yield boto3.client("s3", region_name=os.environ.get("AWS_REGION"))


@pytest.fixture
def configuration_data():
    with open(
        os.path.join(os.path.dirname(__file__), "fixtures", CONFIG_FILE), "r",
    ) as f:
        body = f.read()
    return yaml.safe_load(body)


@pytest.fixture
def sfn_configuration_data():
    with open(
        os.path.join(os.path.dirname(__file__), "fixtures", CONFIG_FILE), "r",
    ) as f:
        body = f.read()

    return {
        "config": yaml.safe_load(body),
        "bucket": "some_bucket",
        "dataset_file": "train/demand.csv",
    }
