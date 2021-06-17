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

import importlib.util
from pathlib import Path

import boto3
import botocore.exceptions
import pytest
from moto import mock_s3, mock_sts

BUILD_S3_CDK_DIST_PATH = str(
    (Path(__file__).parents[2] / "deployment" / "build-s3-cdk-dist.py").absolute()
)


@pytest.fixture
def build_tools():
    spec = importlib.util.spec_from_file_location(
        "build_s3_cdk_dist", BUILD_S3_CDK_DIST_PATH
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@mock_s3
@mock_sts
def test_bucket_check_valid(build_tools):
    s3 = boto3.client("s3", region_name="eu-central-1")
    s3.create_bucket(
        Bucket="MyBucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
    )

    packager = build_tools.BaseAssetPackager()
    packager.s3_asset_path = "s3://MyBucket"
    assert packager.check_bucket()


@mock_s3
@mock_sts
def test_bucket_check_invalid(build_tools):
    packager = build_tools.BaseAssetPackager()
    packager.s3_asset_path = "s3://MyBucket"

    with pytest.raises(botocore.exceptions.ClientError):
        assert packager.check_bucket()
