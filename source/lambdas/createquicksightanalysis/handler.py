# #####################################################################################################################
#  Copyright 2020-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                       #
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

from os import environ

from shared.Dataset.dataset_file import DatasetFile
from shared.config import Config
from shared.logging import get_logger
from shared.quicksight_custom_resources.quicksight import QuickSight

logger = get_logger(__name__)


def createquicksightanalysis(event, context):
    """
    Create consolidated export tables for forecast visualization
    :param event: lambda event
    :param context: lambda context
    :return: glue table name
    """
    config = Config.from_sfn(event)

    dataset_file = DatasetFile(event.get("dataset_file"), event.get("bucket"))
    dataset_group_name = event.get("dataset_group_name")
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
