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

import pytest

from shared.Dataset.dataset_file import DatasetFile
from shared.Dataset.dataset_type import DatasetType


@pytest.fixture(scope="module")
def dataset_target():
    return "some/s3/path/train/some_filename.csv"


@pytest.fixture(scope="module")
def dataset_related():
    return "some/s3/path/train/some_filename.related.csv"


@pytest.fixture(scope="module")
def dataset_metadata():
    return "some/s3/path/train/some_filename.metadata.csv"


@pytest.fixture(scope="module")
def bucket():
    return "somebucket"


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
