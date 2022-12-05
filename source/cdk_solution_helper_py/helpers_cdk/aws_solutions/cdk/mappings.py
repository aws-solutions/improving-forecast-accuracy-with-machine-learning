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
from typing import Optional

from aws_cdk.core import Construct, CfnMapping


class Mappings:
    def __init__(
        self,
        parent: Construct,
        solution_id: str,
        send_anonymous_usage_data: bool = True,
        extra_mappings: Optional[dict] = None,
    ):
        self.parent = parent

        # Track the solution mapping (ID, version, anonymous usage data)
        self.solution_mapping = CfnMapping(
            parent,
            "Solution",
            mapping={
                "Data": {
                    "ID": solution_id,
                    "Version": "%%SOLUTION_VERSION%%",
                    "SendAnonymousUsageData": "Yes" if send_anonymous_usage_data else "No",
                    "SolutionName": "%%SOLUTION_NAME%%",
                    "AppRegistryName": "%%APP_REG_NAME%%",
                    "ApplicationType": "AWS-Solutions",
                }
            },
        )

        # track the s3 bucket, key prefix and (optional) quicksight template source
        general = {
            "S3Bucket": "%%BUCKET_NAME%%",
            "KeyPrefix": "%%SOLUTION_NAME%%/%%SOLUTION_VERSION%%",
        }
        if extra_mappings:
            general.update(extra_mappings)

        self.source_mapping = CfnMapping(
            parent,
            "SourceCode",
            mapping={"General": general},
        )
