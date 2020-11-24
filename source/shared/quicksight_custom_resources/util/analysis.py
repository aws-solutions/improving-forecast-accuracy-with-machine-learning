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
from tenacity import retry_if_exception_type, retry, stop_after_attempt

from shared.helpers import get_quicksight_client
from shared.logging import get_logger
from shared.quicksight_custom_resources.util.quicksight_resource import (
    QuickSightResource,
    QuickSightFailure,
)
from shared.quicksight_custom_resources.util.source_entity import SourceEntity

logger = get_logger(__name__)


class Analysis(QuickSightResource):
    def __init__(
        self,
        quicksight_application=None,
        data_sets=None,
        quicksight_template_arn=None,
        data_source=None,
        props=None,
    ):
        super().__init__(quicksight_application=quicksight_application, props=props)
        self.type = "analysis"
        self.use_props(props)

        self.data_sets = data_sets
        self.data_source = data_source
        self.quicksight_template_arn = quicksight_template_arn

        self.config_data = dict()
        self._load_config(self.type, ["main"], self.config_data)
        self.source_entity = SourceEntity(
            data_sets,
            quicksight_template_arn,
            self.config_data,
            source_entity_type="SourceTemplate",
        )

    @retry(retry=retry_if_exception_type(QuickSightFailure), stop=stop_after_attempt(3))
    def create(self):
        logger.info(f"requesting quicksight create_analysis: {self.id}")
        quicksight_client = get_quicksight_client()

        # TODO: would prefer a generic name here
        source_entity = self._get_source_entity()
        source_entity["SourceTemplate"]["DataSetReferences"][0][
            "DataSetPlaceholder"
        ] = "export_ts_2020_10_19_16_28_06"

        self.data_source.update()
        try:
            response = quicksight_client.create_analysis(
                AwsAccountId=self.aws_account_id,
                AnalysisId=self.id,
                Name=self.name,
                Permissions=self._get_permissions(),
                SourceEntity=source_entity,
            )
            logger.info(
                f"finished quicksight create_analysis for id:{self.id} "
                f"response: {response}"
            )
        except quicksight_client.exceptions.ResourceExistsException:
            response = quicksight_client.describe_analysis(
                AwsAccountId=self.aws_account_id, AnalysisId=self.id
            )
            response = response["Analysis"]
        except quicksight_client.exceptions.InvalidParameterValueException as exc:
            logger.error(str(exc))
            raise QuickSightFailure()

        self.arn = response["Arn"]
        return response

    def delete(self):
        logger.info(f"requesting quicksight delete_analysis id:{self.id}")
        quicksight_client = get_quicksight_client()

        response = quicksight_client.delete_analysis(
            AwsAccountId=self.aws_account_id, AnalysisId=self.id
        )
        logger.info(
            f"finished quicksight delete_analysis for id:{self.id} "
            f"response: {response}"
        )
        return response

    def _get_permissions(self):
        # The principal is the owner of the resource and create the resources and is given full actions for the type
        permissions = [
            {
                "Principal": self.principal_arn,
                "Actions": [
                    "quicksight:RestoreAnalysis",
                    "quicksight:UpdateAnalysisPermissions",
                    "quicksight:DeleteAnalysis",
                    "quicksight:QueryAnalysis",
                    "quicksight:DescribeAnalysisPermissions",
                    "quicksight:DescribeAnalysis",
                    "quicksight:UpdateAnalysis",
                ],
            }
        ]
        return permissions

    def _get_source_entity(self):
        return self.source_entity.get_source_entity()
