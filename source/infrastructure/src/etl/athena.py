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
from aws_cdk.aws_s3 import IBucket
from aws_cdk.core import Construct, CfnResource, Aws


class Athena(Construct):
    def __init__(self, scope: Construct, id: str, athena_bucket: IBucket):
        super().__init__(scope, id)

        workgroup_logical_id = "AthenaWorkGroup"
        self.workgroup = CfnResource(
            self,
            workgroup_logical_id,
            type="AWS::Athena::WorkGroup",
            properties={
                "Name": Aws.STACK_NAME,
                "Description": f"Workgroup for Improving Forecast Accuracy with Machine Learning (stack: {Aws.STACK_NAME})",
                "State": "ENABLED",
                "RecursiveDeleteOption": True,
                "WorkGroupConfiguration": {
                    "EnforceWorkGroupConfiguration": True,
                    "ResultConfiguration": {
                        "OutputLocation": athena_bucket.s3_url_for_object(
                            "query-results"
                        ),
                        "EncryptionConfiguration": {"EncryptionOption": "SSE_S3"},
                    },
                },
            },
        )
        self.workgroup.override_logical_id(workgroup_logical_id)
