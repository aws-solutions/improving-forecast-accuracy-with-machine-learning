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

from os import environ

from shared.logging import get_logger
from shared.quicksight_custom_resources.quicksight import QuickSight

logger = get_logger(__name__)


def createquicksightanalysis(event, context):  # NOSONAR - context is a required argument for this function
    # Removing context could potentially cause breaking changes in deployment
    """
    Create consolidated export tables for forecast visualization
    :param event: lambda event
    :param context: lambda context
    :return: glue table name
    """
    table_name = event.get("glue_table_name")

    workgroup = environ.get("WORKGROUP_NAME")
    schema = environ.get("SCHEMA_NAME")
    principal = environ.get("QUICKSIGHT_PRINCIPAL")
    source_template = environ.get("QUICKSIGHT_SOURCE")

    # attempt to create QuickSight analysis
    qs = QuickSight(
        workgroup=workgroup,
        table_name=table_name,
        schema=schema,
        principal=principal,
        source_template=source_template,
    )
    qs.create_data_source()
    qs.create_data_set()
    qs.create_analysis()
