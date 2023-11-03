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

from shared.logging import get_logger

from shared.quicksight_custom_resources.util.quicksight_application import (
    QuicksightApplication,
)
from shared.quicksight_custom_resources.util.template import TemplatePermissionType

logger = get_logger(__name__)


class QuicksightApi:
    def __init__(self, resource_properties):
        self.quicksight_application = QuicksightApplication(resource_properties)
        self.global_state = self.quicksight_application.get_global_state()

    def create_data_source(self):
        qs_resource = self.quicksight_application.get_data_source()
        response = qs_resource.create()
        self.get_global_state().update({"datasource": qs_resource.get_data()})
        return response

    def create_data_sets(self):
        responses = []

        data_set_sub_types = (
            self.quicksight_application.get_supported_data_set_sub_types()
        )
        data_sets = self.quicksight_application.get_data_sets()
        self.get_global_state().update({"dataset": {}})
        for data_set_type in data_set_sub_types:
            response = data_sets[data_set_type].create()
            responses.append(response)
            self.get_global_state()["dataset"].update(
                {data_set_type: data_sets[data_set_type].get_data()}
            )

        return responses

    def create_analysis(self):
        qs_resource = self.quicksight_application.get_analysis()
        response = qs_resource.create()
        self.get_global_state().update({"analysis": qs_resource.get_data()})
        return response

    def create_dashboard(self):
        qs_resource = self.quicksight_application.get_dashboard()
        response = qs_resource.create()
        self.get_global_state().update({"dashboard": qs_resource.get_data()})
        return response

    def delete_data_source(self):
        qs_resource = self.quicksight_application.get_data_source()
        response = qs_resource.delete()
        return response

    def delete_data_sets(self):
        responses = []

        data_sets = self.quicksight_application.get_data_sets()
        for data_set in data_sets.values():
            response = data_set.delete()
            responses.append(response)
        return responses

    def delete_analysis(self):
        qs_resource = self.quicksight_application.get_analysis()
        response = qs_resource.delete()
        return response

    def delete_dashboard(self):
        qs_resource = self.quicksight_application.get_dashboard()
        response = qs_resource.delete()
        return response

    def create_template_from_template(self, source_template_arn):
        qs_resource = self.quicksight_application.get_template()
        response = qs_resource.create_from_template(source_template_arn)
        self.get_global_state().update({"template": qs_resource.get_data()})
        return response

    def create_template_from_analysis(self):
        template = self.quicksight_application.get_template()
        analysis = self.quicksight_application.get_analysis()
        response = template.create_from_analysis(analysis)
        self.get_global_state().update({"template": template.get_data()})
        return response

    def create_template_from_dashboard(self):
        template = self.quicksight_application.get_template()
        dashboard = self.quicksight_application.get_dashboard()
        response = template.create_from_dashboard(dashboard)
        self.get_global_state().update({"template": template.get_data()})
        return response

    def update_template_permissions(
        self,
        permission: TemplatePermissionType = TemplatePermissionType.PUBLIC,
        principal=None,
    ):
        qs_resource = self.quicksight_application.get_template()
        response = qs_resource.update_template_permissions(permission, principal)
        return response

    def delete_template(self):
        qs_resource = self.quicksight_application.get_template()
        response = qs_resource.delete()
        return response

    def get_global_state(self):
        return self.global_state
