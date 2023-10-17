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

from constructs import Construct
from aws_cdk.aws_s3 import LifecycleRule
from aws_cdk import Duration

from aws_solutions.cdk.utils.secure_bucket import SecureBucket

class DataBucket(SecureBucket):
    def __init__(self, scope: Construct, **kwargs):
        super().__init__(
            scope,
            "ForecastBucket",
            lifecycle_rules=[
                LifecycleRule(
                    abort_incomplete_multipart_upload_after=Duration.days(3),
                    enabled=True,
                ),
                LifecycleRule(expiration=Duration.days(1), prefix="raw/", enabled=True),
            ],
            **kwargs
        )
