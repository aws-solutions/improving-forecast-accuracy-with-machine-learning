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

logger = get_logger(__name__)


class SourceEntity:
    supported_source_entity_types = ["SourceTemplate", "SourceAnalysis"]

    def __init__(self, data_sets, source_obj_arn, config_data, source_entity_type):
        self.data_sets = data_sets
        self.source_obj_arn = source_obj_arn
        self.config_data = config_data
        if source_entity_type not in self.supported_source_entity_types:
            raise ValueError(
                f"Invalid source_entity_type {source_entity_type}, "
                f"valid values are {self.supported_source_entity_types}"
            )
        self.source_entity_type = source_entity_type

    def get_source_entity(self):
        sub_type = "main"
        source_entity = self._get_map(sub_type, "SourceEntity")
        self._update_source_entity(source_entity)
        return source_entity

    def _update_source_entity(self, obj):
        """Update DataSetArn values in SourceEntity"""
        # TODO:ERROR_HANDLING: assert, handle, raise, passthrough
        source_object = obj.get(self.source_entity_type, None)
        assert source_object
        logger.debug(
            f"Initial value of sourceEntity.sourceTemplate.arn: {source_object['Arn']}"
        )
        source_object["Arn"] = self.source_obj_arn
        logger.debug(
            f"Updated value of sourceEntity.sourceTemplate.arn: {source_object['Arn']}"
        )
        data_set_references = source_object.get("DataSetReferences", None)
        assert source_object

        for ds_ref in data_set_references:
            dsr_placeholder = ds_ref.get("DataSetPlaceholder", None)
            dsr_arn = ds_ref.get("DataSetArn", None)
            logger.debug(
                f"Initial value of DataSetReferences, "
                f"DataSetPlaceholder: {dsr_placeholder}, DataSetArn: {dsr_arn}"
            )
            data_set = self.data_sets.get(dsr_placeholder, None)
            assert data_set
            ds_ref["DataSetPlaceholder"] = data_set.name
            ds_ref["DataSetArn"] = data_set.arn
            logger.debug(
                f"Updated value of DataSetReferences, "
                f"DataSetPlaceholder: {ds_ref['DataSetPlaceholder']}, DataSetArn: {ds_ref['DataSetArn']}"
            )

    # TODO:RENAME _get_map to something better and add pydoc
    def _get_map(self, sub_type, map_type):
        if sub_type not in self.config_data:
            raise ValueError(f"Unknown sub type {sub_type}.")
        sub_type_config = self.config_data[sub_type]
        if map_type not in sub_type_config:
            raise ValueError(
                f"Missing {map_type} in config of data set type {sub_type}."
            )
        return sub_type_config[map_type]
