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


SCHEMAS_DEF = {
    "RETAIL": {
        "fields": ["item_id", "timestamp", "demand"],
        "identifier": "item_id",
        "metric": "demand",
    },
    "CUSTOM": {
        "fields": ["item_id", "timestamp", "target_value"],
        "identifier": "item_id",
        "metric": "target_value",
    },
    "INVENTORY_PLANNING": {
        "fields": ["item_id", "timestamp", "demand"],
        "identifier": "item_id",
        "metric": "demand",
    },
    "EC2_CAPACITY": {
        "fields": ["instance_type", "timestamp", "number_of_instances"],
        "identifier": "instance_type",
        "metric": "number_of_instances",
    },
    "WORK_FORCE": {
        "fields": ["workforce_type", "timestamp", "workforce_demand"],
        "identifier": "workforce_type",
        "metric": "workforce_demand",
    },
    "WEB_TRAFFIC": {
        "fields": ["item_id", "timestamp", "value"],
        "identifier": "item_id",
        "metric": "value",
    },
    "METRICS": {
        "fields": ["metric_name", "timestamp", "metric_value"],
        "identifier": "metric_name",
        "metric": "metric_value",
    },
}
