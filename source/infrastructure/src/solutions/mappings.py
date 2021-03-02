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

from aws_cdk.core import Construct, CfnMapping


class Mappings:
    def __init__(
        self,
        parent: Construct,
        solution_id: str,
        send_anonymous_usage_data: bool = True,
        quicksight_template_arn: bool = False,
    ):
        self.parent = parent

        # Track the solution mapping (ID, version, anonymous usage data)
        self.solution_mapping = CfnMapping(
            parent,
            "Solution",
            mapping={
                "Data": {
                    "ID": solution_id,
                    "Version": "%%VERSION%%",
                    "SendAnonymousUsageData": "Yes"
                    if send_anonymous_usage_data
                    else "No",
                }
            },
        )

        # track the s3 bucket, key prefix and (optional) quicksight template source
        general = {
            "S3Bucket": "%%BUCKET_NAME%%",
            "KeyPrefix": "%%SOLUTION_NAME%%/%%VERSION%%",
        }
        if quicksight_template_arn:
            general["QuickSightSourceTemplateArn"] = "%%QUICKSIGHT_SOURCE%%"

        self.source_mapping = CfnMapping(
            parent,
            "SourceCode",
            mapping={"General": general},
        )
