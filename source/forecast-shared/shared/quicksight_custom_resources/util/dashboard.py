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

from shared.helpers import get_quicksight_client
from shared.logging import get_logger
from shared.quicksight_custom_resources.util.quicksight_resource import (
    QuickSightResource,
)
from shared.quicksight_custom_resources.util.source_entity import SourceEntity

logger = get_logger(__name__)


class Dashboard(QuickSightResource):
    def __init__(
        self,
        quicksight_application=None,
        data_sets=None,
        quicksight_template_arn=None,
        props=None,
    ):
        super().__init__(quicksight_application=quicksight_application, props=props)
        self.type = "dashboard"
        self.use_props(props)

        self.data_sets = data_sets
        self.quicksight_template_arn = quicksight_template_arn

        self.config_data = dict()
        self._load_config(self.type, ["main"], self.config_data)
        self.source_entity = SourceEntity(
            data_sets,
            quicksight_template_arn,
            self.config_data,
            source_entity_type="SourceTemplate",
        )

    def create(self):
        logger.info(f"requesting quicksight create_dashboard: {self.id}")
        quicksight_client = get_quicksight_client()

        response = quicksight_client.create_dashboard(
            AwsAccountId=self.aws_account_id,
            DashboardId=self.id,
            Name=self.name,
            Permissions=self._get_permissions(),
            SourceEntity=self._get_source_entity(),
            DashboardPublishOptions=self._get_dashboard_publish_options(),
        )
        logger.info(
            f"finished quicksight create_dashboard for id:{self.id} "
            f"response: {response}"
        )

        self.arn = response["Arn"]
        return response

    def delete(self):
        logger.info(f"requesting quicksight delete_dashboard id:{self.id}")
        quicksight_client = get_quicksight_client()

        response = quicksight_client.delete_dashboard(
            AwsAccountId=self.aws_account_id, DashboardId=self.id
        )
        logger.info(
            f"finished quicksight delete_dashboard for id:{self.id} "
            f"response: {response}"
        )

        return response

    def _get_dashboard_publish_options(self):
        dashboard_publish_options = {
            "AdHocFilteringOption": {"AvailabilityStatus": "ENABLED"},
            "ExportToCSVOption": {"AvailabilityStatus": "ENABLED"},
            "SheetControlsOption": {"VisibilityState": "EXPANDED"},
        }
        return dashboard_publish_options

    def _get_permissions(self):
        # The principal is the owner of the resource and create the resources and is given full actions for the type
        permissions = [
            {
                "Principal": self.principal_arn,
                "Actions": [
                    "quicksight:DescribeDashboard",
                    "quicksight:ListDashboardVersions",
                    "quicksight:UpdateDashboardPermissions",
                    "quicksight:QueryDashboard",
                    "quicksight:UpdateDashboard",
                    "quicksight:DeleteDashboard",
                    "quicksight:DescribeDashboardPermissions",
                    "quicksight:UpdateDashboardPublishedVersion",
                ],
            }
        ]
        return permissions

    def _get_source_entity(self):
        return self.source_entity.get_source_entity()
