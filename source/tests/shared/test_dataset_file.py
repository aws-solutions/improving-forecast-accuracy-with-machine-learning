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

import datetime
from hashlib import md5

import boto3
import numpy as np
import pytest
from dateutil.tz import tzutc
from freezegun import freeze_time
from moto import mock_s3

from shared.Dataset.dataset_file import DatasetFile
from shared.Dataset.dataset_type import DatasetType


@pytest.fixture
def dataset_target():
    return "some/s3/path/train/some_filename.csv"


@pytest.fixture
def dataset_related():
    return "some/s3/path/train/some_filename.related.csv"


@pytest.fixture
def dataset_metadata():
    return "some/s3/path/train/some_filename.metadata.csv"


@pytest.fixture
def bucket():
    return "somebucket"


@pytest.fixture
def dataset_file(dataset_target, bucket):
    dsf = DatasetFile(dataset_target, bucket)

    with mock_s3():
        cli = boto3.client("s3")
        cli.create_bucket(Bucket=bucket)

        yield dsf


def test_target_timeseries_file(dataset_target, bucket):
    assert (
        DatasetFile(dataset_target, bucket).data_type == DatasetType.TARGET_TIME_SERIES
    )


def test_related_timeseries_file(dataset_related, bucket):
    assert (
        DatasetFile(dataset_related, bucket).data_type
        == DatasetType.RELATED_TIME_SERIES
    )


def test_metadata_file(dataset_metadata, bucket):
    assert DatasetFile(dataset_metadata, bucket).data_type == DatasetType.ITEM_METADATA


def test_dataset_name(dataset_target, bucket):
    dsf = DatasetFile(dataset_target, bucket)
    assert dsf.name == "some_filename"
    assert dsf.data_type == DatasetType.TARGET_TIME_SERIES


def test_dataset_name_metadata(dataset_metadata, bucket):
    dsf = DatasetFile(dataset_metadata, bucket)
    assert dsf.prefix == "some_filename"
    assert dsf.name == "some_filename_metadata"
    assert dsf.data_type == DatasetType.ITEM_METADATA


def test_dataset_name_related(dataset_related, bucket):
    dsf = DatasetFile(dataset_related, bucket)
    assert dsf.name == "some_filename_related"
    assert dsf.prefix == "some_filename"
    assert dsf.data_type == DatasetType.RELATED_TIME_SERIES


@pytest.mark.parametrize(
    "path,bucket,key",
    [
        ("s3://some_bucket/some_key", "some_bucket", "some_key"),
        ("s3://some_bucket/", "some_bucket", ""),
        ("s3://some_bucket/some_key?query=string", "some_bucket", "some_key"),
    ],
)
def test_dataset_file_from_s3_path(path, bucket, key):
    dsf = DatasetFile.from_s3_path(s3_path=path)
    assert dsf.bucket == bucket
    assert dsf.key == key


@pytest.mark.parametrize(
    "path",
    [
        ("s3://some_bucket/some_key"),
        ("s3://some_bucket/"),
        ("s3://some_bucket/some_key?query=string"),
    ],
)
def test_dataset_file_from_s3_path(path):
    dsf = DatasetFile.from_s3_path(s3_path=path)
    assert dsf.s3_url == path.split("?")[0]


@pytest.mark.parametrize(
    "path",
    [
        ("s3://some_bucket/some_key"),
        ("s3://some_bucket/"),
        ("s3://some_bucket/some_key?query=string"),
    ],
)
def test_dataset_file_from_s3_path(path):
    dsf = DatasetFile.from_s3_path(s3_path=path)
    assert dsf.s3_prefix == "s3://some_bucket/"


def test_dataset_file_from_s3_path():
    dsf = DatasetFile.from_s3_path(s3_path="s3://bucket/a key/with some spaces .txt")
    assert dsf.bucket == "bucket"
    assert dsf.key == "a key/with some spaces .txt"


def test_s3_file_hash(dataset_target, bucket, dataset_file):
    # create some random data to be checksummed
    MB = 1024 * 1024
    random_data = np.random.bytes(25 * MB)
    hexdigest = md5(random_data).hexdigest()

    # write the data
    cli = boto3.client("s3")
    cli.put_object(Bucket=bucket, Key=dataset_target, Body=random_data)

    assert dataset_file.etag == hexdigest


def test_last_updated(dataset_target, bucket, dataset_file):
    cli = boto3.client("s3")
    now = datetime.datetime.now(tzutc()).replace(microsecond=0)

    dataset_file.cli = cli
    with freeze_time(now):
        cli.put_object(Bucket=bucket, Key=dataset_target, Body="SOME_DATA")
    assert dataset_file.last_updated == now
