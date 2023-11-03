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

import time
from datetime import datetime
from functools import lru_cache
from hashlib import md5
from os.path import split, join
from urllib.parse import urlparse

from shared.Dataset.dataset_type import DatasetType
from shared.helpers import get_s3_client
from shared.logging import get_logger

logger = get_logger(__name__)


class DatasetFile:
    """Stores characteristics of a dataset file uploaded for ingestion by the solution"""

    def __init__(self, key: str, bucket: str):
        self.key = key
        self.bucket = bucket
        self.cli = get_s3_client()

        if key.endswith(".related.csv"):
            self.data_type = DatasetType.RELATED_TIME_SERIES
        elif key.endswith(".metadata.csv"):
            self.data_type = DatasetType.ITEM_METADATA
        else:
            self.data_type = DatasetType.TARGET_TIME_SERIES

        _, self.filename = split(key)

    @classmethod
    def from_s3_path(cls, s3_path):
        """
        Used to create a DatasetFile from a fully qualified S3 path
        :param event: The S3 URL of of the dataset file
        :return: DatasetFile
        """
        parsed = urlparse(s3_path, allow_fragments=False)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")
        dsf = DatasetFile(bucket=bucket, key=key)
        return dsf

    @property
    def s3_url(self):
        return f"s3://{self.bucket}/{self.key}"

    @property
    def s3_prefix(self):
        return "/".join(self.s3_url.split("/")[:-1]) + "/"

    def copy(self, path: str, *args: str) -> str:
        """
        Copy this Datasetfile to another key <path>/<subpath>/<original_name> in the same bucket
        :param path:
        :param subpath:
        :return: s3 key without filename or extension for the copied file
        """
        dest = join(path, *args, self.filename)

        logger.info(f"copying {self.name} to s3://{self.bucket}/{dest}")
        self.cli.copy({"Bucket": self.bucket, "Key": self.key}, self.bucket, dest)

        return dest

    @property
    def name(self):
        """The name of the dataset (including _related or _metadata for those dataset types)"""
        name = next(iter(self.filename.split(".")))
        if self.data_type == DatasetType.RELATED_TIME_SERIES:
            name += "_related"
        elif self.data_type == DatasetType.ITEM_METADATA:
            name += "_metadata"
        return name

    @property
    def prefix(self):
        """The prefix of the dataset (not including _related or _metadata for those dataset types)"""
        prefix = next(iter(self.filename.split(".")))
        return prefix

    @property
    @lru_cache()
    def last_updated(self) -> datetime:
        obj_info = self.cli.get_object(Bucket=self.bucket, Key=self.key)
        return obj_info.get("LastModified")

    @property
    @lru_cache()
    def etag(self) -> str:
        """
        Get the full MD5 signature of an S3 object (streams in 10MB increments)
        :return:
        """
        signature = md5() # nosec # NOSONAR - this hashing data is safe here
        s3_object = self.cli.get_object(Bucket=self.bucket, Key=self.key)

        logger.info(f"getting signature for s3://{self.bucket}/{self.key}")
        time_start = time.perf_counter()
        for chunk in s3_object["Body"].iter_chunks(10240):
            signature.update(chunk)
        hexdigest = signature.hexdigest()
        time_end = time.perf_counter()
        logger.info(f"signature calculated in {time_end - time_start:0.4f} seconds")

        return hexdigest

    def __repr__(self):
        return f"DatasetFile(key='{self.key}',bucket='{self.bucket}')"
