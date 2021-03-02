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
from dataclasses import dataclass
from typing import Union

import pytest

from lambdas.cloudformation_resources.url_helper import (
    url_info,
    helper,
    UrlHelper,
)


@dataclass
class ValidUrls:
    url: str
    bucket: str
    key: str


def valid_s3_urls():
    @dataclass
    class ValidUrl:
        url: str
        bucket: Union[str, None]
        key: Union[str, None]
        scheme: str

    return [
        ValidUrl(
            "https://s3.us-east-1.amazonaws.com/bucket-name/key-name",
            "bucket-name",
            "key-name",
            "s3",
        ),
        ValidUrl(
            "https://s3-us-east-1.amazonaws.com/bucket-name/key-name",
            "bucket-name",
            "key-name",
            "s3",
        ),
        ValidUrl(
            "https://bucket-name.s3.us-west-2.amazonaws.com/key-name",
            "bucket-name",
            "key-name",
            "s3",
        ),
        ValidUrl(
            "https://bucket-name.s3-us-west-2.amazonaws.com/key-name",
            "bucket-name",
            "key-name",
            "s3",
        ),
        ValidUrl(
            "https://bucket-name.s3.amazonaws.com/key-name",
            "bucket-name",
            "key-name",
            "s3",
        ),
        ValidUrl("s3://bucket-name/key-name", "bucket-name", "key-name", "s3"),
        ValidUrl("http://somedomain.com/some_file.csv", "", "", "http"),
        ValidUrl("https://somedomain.com/some_file.csv", "", "", "https"),
    ]


@pytest.mark.parametrize("valid_url", valid_s3_urls())
def test_copy_helper_buckets_and_keys(valid_url):
    helper = UrlHelper(url=valid_url.url)

    assert helper.scheme == valid_url.scheme
    assert helper.source_bucket == valid_url.bucket
    assert helper.source_key == valid_url.key


@pytest.mark.parametrize("valid_url", valid_s3_urls())
def test_generate_name(valid_url):
    event = {
        "ResourceProperties": {
            "Url": valid_url.url,
        }
    }

    url_info(event, None)

    assert helper.Data["Url"] == valid_url.url
    assert helper.Data["Scheme"] == valid_url.scheme
    if valid_url.scheme == "s3":
        assert helper.Data["Bucket"] == valid_url.bucket
        assert helper.Data["Key"] == valid_url.key
    else:
        assert helper.Data.get("Bucket") == ""
        assert helper.Data.get("Key") == ""
