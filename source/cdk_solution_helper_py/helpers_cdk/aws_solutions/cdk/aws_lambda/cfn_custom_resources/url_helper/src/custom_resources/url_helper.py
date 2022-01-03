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
import logging
import re
from dataclasses import dataclass, field
from os import getenv
from typing import Union
from urllib.parse import urlparse

from crhelper import CfnResource

logger = logging.getLogger(__name__)
helper = CfnResource(log_level=getenv("LOG_LEVEL", "WARNING"))


def get_property(event, property_name, property_default=None, property_required=True):
    _prop = event.get("ResourceProperties", {}).get(property_name, property_default)
    if not _prop and property_required:
        raise ValueError(f"missing required property {property_name}")
    return _prop


@dataclass
class UrlHelper:
    url: str
    scheme: str = field(init=False, repr=False)
    source_bucket: Union[str, None] = field(init=False, repr=False, default="")
    source_key: Union[str, None] = field(init=False, repr=False, default="")

    def __post_init__(self):
        parse_result = urlparse(self.url, allow_fragments=False)

        self.scheme = parse_result.scheme.lower()
        if self.scheme == "s3":
            self.scheme = "s3"
            self.source_bucket = parse_result.netloc
            self.source_key = parse_result.path.lstrip("/")
        elif self.scheme == "http" or self.scheme == "https":
            s3_matchers = [
                r"^https?://s3[.-](.*).amazonaws.com/(?P<bucket>.*)/(?P<key>.*)$",
                r"^https?://(?P<bucket>.*).s3[.-](.*).amazonaws.com/(?P<key>.*)$",
                r"^https?://(?P<bucket>.*).s3.amazonaws.com/(?P<key>.*)$",
            ]

            # detect S3 HTTP/ HTTPS URLS
            for regex in s3_matchers:
                match = re.search(regex, self.url)
                if match:
                    self.scheme = "s3"
                    self.source_bucket = match.group("bucket")
                    self.source_key = match.group("key")
                    break
        else:
            raise ValueError("URL scheme %s is not supported" % self.scheme)


@helper.create
def url_info(event, _):
    """
    Determine URL schema (S3 or otherwise)

    :param event: The CloudFormation custom resource event
    :return: None
    """
    source_url = get_property(event, "Url")
    url_helper = UrlHelper(source_url)

    helper.Data["Url"] = source_url
    helper.Data["Scheme"] = url_helper.scheme
    helper.Data["Bucket"] = url_helper.source_bucket
    helper.Data["Key"] = url_helper.source_key


@helper.update
@helper.delete
def no_op(_, __):
    pass  # pragma: no cover


def handler(event, _):
    """
    Handler entrypoint - see url_info for implementation details
    :param event: The CloudFormation custom resource event
    :return: PhysicalResourceId
    """
    helper(event, _)  # pragma: no cover
