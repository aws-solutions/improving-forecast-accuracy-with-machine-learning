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
from functools import cached_property
from os.path import split

from shared.Dataset.dataset_type import DatasetType
from shared.helpers import get_s3_client


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

    @cached_property
    def size(self) -> int:
        """
        Get the size of the dataset in lines (using S3 select)
        :return: the size of the dataset in lines
        """

        # This query counts lines that are not blank (have at least one item)
        query = f"select count(*) as lines from s3object s where s._1 != ''"

        select = self.cli.select_object_content(
            Bucket=self.bucket,
            Key=self.key,
            ExpressionType="SQL",
            Expression=query,
            InputSerialization={"CSV": {"FileHeaderInfo": "NONE"}},
            OutputSerialization={"JSON": {}},
        )

        for event in select["Payload"]:
            if "Records" in event:
                records = event["Records"]["Payload"].decode("utf-8")

        return json.loads(records).get("lines")

    def __repr__(self):
        return f"DatasetFile(key='{self.key}',bucket='{self.bucket}')"
