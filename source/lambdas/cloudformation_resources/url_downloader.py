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
import logging
import threading
import time
from dataclasses import dataclass, field
from os import getenv

import boto3
import requests
from boto3.s3.transfer import TransferConfig, MB
from crhelper import CfnResource

logger = logging.getLogger(__name__)
helper = CfnResource(log_level=getenv("LOG_LEVEL", "WARNING"))


def get_property(event, property_name, property_default=None, property_required=True):
    property = event.get("ResourceProperties", {}).get(property_name, property_default)
    if not property and property_required:
        raise ValueError(f"missing required property {property_name}")
    return property


class ProgressTracker:
    """Used to track download progress for HTTP/ HTTPS downloads"""

    def __init__(self, content_length):
        self.content_length = int(content_length) if content_length else None
        self.seen = 0
        self._lock = threading.Lock()
        self.started = time.time()

    def __call__(self, bytes_amount):
        with self._lock:
            self.seen += bytes_amount
            megabits_per_second = self.seen / 125000 / (time.time() - self.started)
            if self.content_length:
                percentage = (self.seen / self.content_length) * 100
                logger.info(
                    "transferred %d / %d bytes (%.2f%%) transfer speed: %.2f Mbit/s"
                    % (self.seen, self.content_length, percentage, megabits_per_second)
                )
            else:
                logger.info("transfer speed: %.2f Mbit/s" % megabits_per_second)


@dataclass
class Downloader:
    destination_bucket: str
    destination_key: str
    scheme: str
    source_url: str = field(default=None, repr=False)
    source_bucket: str = field(default=None, repr=False)
    source_key: str = field(default=None, repr=False)
    source_filename: str = field(default=None)

    def __post_init__(self):
        if self.scheme == "s3":
            self.copy_from_s3()
        elif self.scheme == "http" or self.scheme == "https":
            self.copy_from_url()
        else:
            raise ValueError("unsupported scheme %s" % self.scheme)

    def copy_from_s3(self):
        errors = []
        if not self.source_bucket:
            errors.append("missing source bucket")
        if not self.source_key:
            errors.append("missing source key")
        if errors:
            raise ValueError(
                "validation error occurred for s3 copy: %s" % ", ".join(errors)
            )

        s3 = boto3.resource("s3")
        dest_bucket = s3.Bucket(self.destination_bucket)
        copy_source = {"Bucket": self.source_bucket, "Key": self.source_key}
        logging.info(
            "copying s3://%s/%s to s3://%s/%s"
            % (
                self.source_bucket,
                self.source_key,
                self.destination_bucket,
                self.destination_key,
            )
        )
        dest_bucket.copy(copy_source, self.destination_key)

    def copy_from_url(self):
        if not self.source_url:
            raise ValueError(
                "validation error occurred for url copy: missing source URL"
            )

        s3 = boto3.resource("s3")
        dest_object = s3.Object(self.destination_bucket, self.destination_key)
        transfer_config = TransferConfig(
            multipart_threshold=8 * MB,
            max_concurrency=4,
            multipart_chunksize=8 * MB,
            use_threads=True,
        )

        with requests.get(self.source_url, stream=True) as response:
            response.raw.decode_content = True
            dest_object.upload_fileobj(
                response.raw,
                Callback=ProgressTracker(
                    content_length=response.headers.get("content-length", None)
                ),
                Config=transfer_config,
            )


@helper.create
def copy_url(event, _):
    """
    Copy file from URL (s3:// http:// or https://)

    :param event: The CloudFormation custom resource event
    :return: None
    """

    # instantiating this performs the download
    copier = Downloader(
        destination_bucket=get_property(event, "DestinationBucket"),
        destination_key=get_property(event, "DestinationKey"),
        scheme=get_property(event, "Scheme"),
        source_bucket=get_property(event, "SourceBucket", property_required=False),
        source_key=get_property(event, "SourceKey", property_required=False),
        source_url=get_property(event, "SourceUrl", property_required=False),
    )


@helper.update
@helper.delete
def no_op(_, __):
    pass  # pragma: no cover


def handler(event, _):
    """
    Handler entrypoint - see copy_url for implementation details
    :param event: The CloudFormation custom resource event
    :return: PhysicalResourceId
    """
    helper(event, _)  # pragma: no cover
