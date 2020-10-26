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
from dataclasses import dataclass, field
from functools import wraps

from shared.logging import get_logger
from shared.quicksight_custom_resources.util.quicksight import QuicksightApi

logger = get_logger(__name__)


def _requires_enterprise(func):
    @wraps(func)
    def wrap(self, *args, **kwargs):
        if self.edition == "ENTERPRISE":
            return func(self, *args, **kwargs)
        else:
            logger.info(
                "quicksight enterprise mode required for %s" % str(func.__name__)
            )
            return

    return wrap


def _requires_principal(func):
    @wraps(func)
    def wrap(self, *args, **kwargs):
        if self.principal:
            return func(self, *args, **kwargs)
        else:
            logger.info("quicksight principal required for %s" % str(func.__name__))
            return

    return wrap


@dataclass
class QuickSight:
    workgroup: str
    schema: str
    table_name: str
    source_template: str
    edition: str = field(init=False)
    api: QuicksightApi = field(init=False)
    principal: str = field(default="")

    @_requires_principal
    def __post_init__(self):
        self.api = QuicksightApi(
            resource_properties={
                "QuickSightPrincipalArn": self.principal,
                "QuickSightSourceTemplateArn": self.source_template,
            }
        )
        self.edition = self.api.quicksight_application.edition

    @_requires_principal
    @_requires_enterprise
    def create_data_source(self):
        # create the data source
        self.api.quicksight_application.data_source.id = f"{self.workgroup}-athena"
        self.api.quicksight_application.data_source.name = f"{self.workgroup}-athena"
        self.api.quicksight_application.data_source.workgroup = f"{self.workgroup}"
        data_source = self.api.create_data_source()

    @_requires_principal
    @_requires_enterprise
    def create_data_set(self):
        # create the data set
        name = f"{self.workgroup}_{self.table_name}"
        self.api.quicksight_application.data_sets["forecast"].id = name
        self.api.quicksight_application.data_sets["forecast"].name = name
        self.api.quicksight_application.data_sets["forecast"].schema = self.schema
        self.api.quicksight_application.data_sets[
            "forecast"
        ].table_name = self.table_name
        self.api.create_data_sets()

    @_requires_principal
    @_requires_enterprise
    def create_analysis(self):
        analysis_id = f"{self.workgroup}_{self.table_name}_analysis"
        analysis_name = f"Forecast Analysis: {self.workgroup}_{self.table_name}"
        self.api.quicksight_application.analysis.id = analysis_id
        self.api.quicksight_application.analysis.name = analysis_name
        self.api.create_analysis()
