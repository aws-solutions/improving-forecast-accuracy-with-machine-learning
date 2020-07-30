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


class BucketNotFound(Exception):
    def __init__(self, *args, **kwargs):
        msg = "Could not get the S3 event notification bucket. Was this an Amazon S3 event notification?"
        if args or kwargs:
            super().__init__(*args, **kwargs)
        else:
            super().__init__(msg)


class RecordNotFound(Exception):
    def __init__(self, *args, **kwargs):
        msg = "Could not get the S3 event notification record. Was this an Amazon S3 event notification?"
        if args or kwargs:
            super().__init__(*args, **kwargs)
        else:
            super().__init__(msg)


class RecordNotSupported(Exception):
    pass


class KeyNotFound(Exception):
    def __init__(self, *args, **kwargs):
        msg = "Could not get the S3 event notification key. Was this an Amazon S3 event notification?"
        if args or kwargs:
            super().__init__(*args, **kwargs)
        else:
            super().__init__(msg)
