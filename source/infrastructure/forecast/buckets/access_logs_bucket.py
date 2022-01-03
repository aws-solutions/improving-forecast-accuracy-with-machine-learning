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
from aws_cdk.core import Construct

from aws_solutions.cdk.cfn_nag import CfnNagSuppression
from aws_solutions.cdk.utils.secure_bucket import SecureBucket


class AccessLogsBucket(SecureBucket):
    def __init__(self, scope: Construct, **kwargs):
        super().__init__(
            scope,
            "AccessLogsBucket",
            suppress=[
                CfnNagSuppression(
                    "W35",
                    "This bucket is used as the logging destination for personalization data processing",
                )
            ],
            **kwargs,
        )
