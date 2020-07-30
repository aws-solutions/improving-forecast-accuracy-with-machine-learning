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

import shared.s3.exceptions as exceptions


def test_recordnotfound_default():
    with pytest.raises(exceptions.RecordNotFound) as exc_info:
        raise exceptions.RecordNotFound

    assert (
        str(exc_info.value)
        == "Could not get the S3 event notification record. Was this an Amazon S3 event notification?"
    )


def test_bucketnotfound_default():
    with pytest.raises(exceptions.BucketNotFound) as exc_info:
        raise exceptions.BucketNotFound

    assert (
        str(exc_info.value)
        == "Could not get the S3 event notification bucket. Was this an Amazon S3 event notification?"
    )


def test_keynotfound_default():
    with pytest.raises(exceptions.KeyNotFound) as exc_info:
        raise exceptions.KeyNotFound

    assert (
        str(exc_info.value)
        == "Could not get the S3 event notification key. Was this an Amazon S3 event notification?"
    )


def test_recordnotfound_message():
    exc_types = [
        exceptions.RecordNotFound,
        exceptions.BucketNotFound,
        exceptions.KeyNotFound,
    ]

    for exc_type in exc_types:
        with pytest.raises(exc_type) as excinfo:
            raise exc_type("Test")

        assert str(excinfo.value) == "Test"
