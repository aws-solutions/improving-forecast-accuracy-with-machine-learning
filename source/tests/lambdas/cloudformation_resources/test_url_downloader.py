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
import json
import logging
import secrets
import time

import boto3
import pytest
from moto import mock_s3

from lambdas.cloudformation_resources.url_downloader import (
    Downloader,
    ProgressTracker,
    get_property,
)


@pytest.fixture
def event():
    return {"ResourceProperties": {"Present": "AccountedFor"}}


def test_get_property_required(event):
    assert get_property(event, "Present") == "AccountedFor"
    assert get_property(event, "Present", "Default?") == "AccountedFor"


def test_get_property_missing(event):
    assert get_property(event, "NotPresent", "NotAccountedFor") == "NotAccountedFor"


def test_get_property_missing_not_required(event):
    assert get_property(event, "NotPresent", property_required=False) == None


def test_get_property_missing_required(event):
    with pytest.raises(ValueError):
        assert get_property(event, "NotPresent")


def test_progress_tracker(mocker, caplog):
    megabit_bytes = 125000
    tracker = ProgressTracker(content_length=megabit_bytes)
    tracker.started = time.mktime(time.gmtime(0))

    # 1st update - one second has passed, should see .5 Mbit/s
    with caplog.at_level(logging.DEBUG):
        mocker.patch("time.time", return_value=time.mktime(time.gmtime(1)))
        tracker(megabit_bytes / 2)
    assert (
        "transferred 62500 / 125000 bytes (50.00%) transfer speed: 0.50 Mbit/s"
        in caplog.messages
    )

    # 2nd update - one more second has passed, complete the upload
    with caplog.at_level(logging.DEBUG):
        mocker.patch("time.time", return_value=time.mktime(time.gmtime(2)))
        tracker(megabit_bytes / 2)
    assert (
        "transferred 125000 / 125000 bytes (100.00%) transfer speed: 0.50 Mbit/s"
        in caplog.messages
    )


@mock_s3
def test_downloader(requests_mock, caplog):
    source_url = "https://test.com/testdata.csv"
    destination_bucket = "dest-bucket"
    destination_key = "train/some.json"
    content_length = 125000
    contents = secrets.token_bytes(content_length)

    # mocked s3 bucket
    cli = boto3.client("s3", region_name="eu-central-1")
    cli.create_bucket(
        Bucket=destination_bucket,
        CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
    )

    # mocked response for download
    requests_mock.get(
        source_url, content=contents, headers={"Content-Length": str(content_length)}
    )

    with caplog.at_level(logging.DEBUG):
        downloader = Downloader(
            destination_bucket=destination_bucket,
            destination_key=destination_key,
            scheme="https",
            source_url=source_url,
        )

    result = cli.get_object(Bucket=destination_bucket, Key=destination_key)

    assert result["Body"].read() == contents


@mock_s3
def test_downloader_https(requests_mock):
    source_url = "https://test.com/testdata.csv"
    destination_bucket = "dest-bucket"
    destination_key = "train/some.json"
    contents = {"Test": "String"}

    # mocked s3 bucket
    cli = boto3.client("s3", region_name="eu-central-1")
    cli.create_bucket(
        Bucket=destination_bucket,
        CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
    )

    # mocked response for download
    requests_mock.get(source_url, json=contents)

    downloader = Downloader(
        destination_bucket=destination_bucket,
        destination_key=destination_key,
        scheme="https",
        source_url=source_url,
    )

    result = cli.get_object(Bucket=destination_bucket, Key=destination_key)
    assert result["Body"].read().decode("utf-8") == json.dumps(contents)
