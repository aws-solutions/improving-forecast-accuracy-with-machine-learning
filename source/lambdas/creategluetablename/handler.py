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

from datetime import datetime

from shared.logging import get_logger

logger = get_logger(__name__)


def creategluetablename(event, context):
    dataset_group_name = event.get("dataset_group_name")
    timestamp = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    glue_table_name = f"{dataset_group_name}_{timestamp}"

    logger.info("glue table name for this forecast: %s" % glue_table_name)
    return glue_table_name
