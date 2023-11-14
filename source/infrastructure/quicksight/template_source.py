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

import logging
import os
from typing import Dict, Optional

logger = logging.getLogger("cdk-helper")


class TemplateSource:
    def __init__(self, solution_name: str, solution_version: str):
        self.solution_name = solution_name
        self.solution_version = solution_version
        self.quicksight_enabled = os.environ.get("ENABLE_QUICKSIGHT", True)
        self.dist_account_id = os.environ.get("DIST_ACCOUNT_ID", None)
        self.dist_quicksight_namespace = os.environ.get(
            "DIST_QUICKSIGHT_NAMESPACE", None
        )

    def _enabled(self):
        return self.quicksight_enabled and self.solution_name and self.solution_version

    @property
    def arn(self) -> Optional[str]:
        if self._enabled():
            quicksight_template_name = "_".join(
                [
                    self.dist_quicksight_namespace,
                    self.solution_name,
                    self.solution_version.replace(".", "_"),
                ]
            )
            return ":".join(
                [
                    "arn:aws:quicksight:us-east-1",
                    self.dist_account_id,
                    f"template/{quicksight_template_name}",
                ]
            )
        else:
            logger.info("QuickSight is not enabled")
            return None

    @property
    def mappings(self) -> Dict:
        if self._enabled():
            return {"QuickSightSourceTemplateArn": self.arn}
        else:
            return {"QuickSightSourceTemplateArn": ""}
