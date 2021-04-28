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

import re

import pytest

import shared.s3.exceptions as exceptions
from shared.s3.notification import Event


@pytest.fixture(scope="function")
def event_missing_records():
    return {}


@pytest.fixture(scope="function")
def event_invalid_version():
    return {"Records": [{"eventVersion": "3.0"}]}


@pytest.fixture(scope="function")
def event_no_bucket():
    return {
        "Records": [
            {
                "eventVersion": "2.2",
            }
        ]
    }


@pytest.fixture(scope="function")
def event_no_key():
    return {
        "Records": [
            {
                "eventVersion": "2.2",
                "s3": {
                    "bucket": {
                        "name": "test-bucket",
                    }
                },
            }
        ]
    }


def test_s3_event_handler_missing_records(event_missing_records):
    with pytest.raises(exceptions.RecordNotFound):
        Event(event_missing_records)


def test_s3_event_handler_invalid_version(event_invalid_version):
    with pytest.raises(exceptions.RecordNotSupported):
        Event(event_invalid_version)


def test_s3_event_handler_no_bucket(event_no_bucket):
    with pytest.raises(exceptions.BucketNotFound):
        Event(event_no_bucket)

    event_no_bucket["Records"][0]["s3"] = {}
    with pytest.raises(exceptions.BucketNotFound):
        Event(event_no_bucket)

    event_no_bucket["Records"][0]["s3"]["bucket"] = {}
    with pytest.raises(exceptions.BucketNotFound):
        Event(event_no_bucket)

    event_no_bucket["Records"][0]["s3"]["bucket"]["name"] = "test-bucket"
    with pytest.raises(exceptions.KeyNotFound):
        Event(event_no_bucket)


def test_s3_event_handler_no_key(event_no_key):
    with pytest.raises(exceptions.KeyNotFound):
        Event(event_no_key)

    event_no_key["Records"][0]["s3"]["object"] = {}
    with pytest.raises(exceptions.KeyNotFound):
        Event(event_no_key)


def test_s3_event_handler_id(event_no_key):
    event_no_key["Records"][0]["s3"]["object"] = {}
    event_no_key["Records"][0]["s3"]["object"]["key"] = "test-key.csv"
    s3_handler = Event(event_no_key)

    assert s3_handler.bucket == "test-bucket"

    id_matcher = re.compile(f"test-key_target_time_series_[0-9a-f]+")
    id = s3_handler.event_id
    assert id_matcher.match(id)
