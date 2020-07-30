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

from uuid import uuid4

from packaging import version

from shared.Dataset.dataset_file import DatasetFile
from shared.s3.exceptions import (
    RecordNotFound,
    RecordNotSupported,
    BucketNotFound,
    KeyNotFound,
)

S3_EVENT_STRUCTURE_MAJOR = 2


class Event:
    def __init__(self, event):
        self.uuid = f"{uuid4().time_low:x}"
        self.bucket, self.key, self.file = self.validate(event)

    def validate(self, event: dict):
        record = next(iter(event.get("Records", [{}])))
        if not record:
            raise RecordNotFound

        # Make sure this event version is supported
        event_version = record.get("eventVersion")
        if version.parse(event_version).major != S3_EVENT_STRUCTURE_MAJOR:
            raise RecordNotSupported(
                f"The event version {event_version} is not supported by this solution."
            )

        # Make sure there's a bucket in the event structure
        bucket = record.get("s3", {}).get("bucket", {}).get("name")
        if not bucket:
            raise BucketNotFound

        # Make sure there's a key in the event structure
        key = record.get("s3", {}).get("object", {}).get("key")
        if not key:
            raise KeyNotFound

        # The name of the event is the stem of the file without extensions
        file = DatasetFile(key=key, bucket=bucket)

        return bucket, key, file

    @property
    def event_id(self) -> str:
        return f"{self.file.prefix}_{str(self.file.data_type).lower()}_{self.uuid}"[
            0:80
        ]
