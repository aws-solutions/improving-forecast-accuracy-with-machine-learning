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
from dataclasses import field, dataclass
from typing import List, Dict, Union

from shared.DatasetGroup.dataset_group import Schema

MAX_DIMENSIONS = 10
MAX_METADATA = 10


@dataclass
class Column:
    term: str = field(default="")
    datatype: str = field(default="")
    alias: str = field(default="")
    expression: str = field(default="")


class Dimension:
    dimension_number: int = 1
    dimension_values: Dict = dict()

    @property
    def alias(self):
        return f"dimension_{self.dimension_number}"

    def __init__(self, name):
        self.name = name
        exists = Dimension.dimension_values.get(name)

        # track the number of user-defined dimensions, ensures consistency across input, export.
        if exists:
            self.dimension_number = Dimension.dimension_values[name]
        else:
            if self.dimension_number > MAX_DIMENSIONS:
                raise ValueError(f"more than {MAX_DIMENSIONS} dimensions detected")
            self.dimension_number = Dimension.dimension_number
            Dimension.dimension_values[name] = self.dimension_number
            Dimension.dimension_number = Dimension.dimension_number + 1


class Metadata:
    metadata_number: int = 1
    metadata_values: Dict = {}

    @property
    def alias(self):
        return f"metadata_{self.metadata_number}"

    def __init__(self, name):
        self.name = name
        exists = Metadata.metadata_values.get(name)

        # track the number of user-defined metadata attributes, ensures consistency across input, export.
        if exists:
            self.metadata_number = Metadata.metadata_values[name]
        else:
            if self.metadata_number > MAX_METADATA:
                raise ValueError(
                    f"more than {MAX_METADATA} metadata attributes detected"
                )
            self.metadata_number = Metadata.metadata_number
            Metadata.metadata_values[name] = self.metadata_number
            Metadata.metadata_number = Metadata.metadata_number + 1


class Model:
    def __init__(self, domain_schema: Schema, user_schema: Union[dict, list]):
        self.domain_schema = domain_schema
        self.columns: List[Column] = list()

        if isinstance(user_schema[0], dict):
            for item in user_schema:
                term = item.get("AttributeName")
                datatype = self._transform_datatype(item.get("AttributeType"))
                alias = self._get_alias(term)

                self.columns.append(Column(term=term, datatype=datatype, alias=alias))
        elif isinstance(user_schema[0], str):
            for item in user_schema:
                term = item
                alias = self._get_alias(term)
                datatype = self._infer_datatype(alias)

                self.columns.append(Column(term=term, datatype=datatype, alias=alias))

    def __getitem__(self, key):
        column = [column for column in self.columns if column.term == key]
        if len(column) != 1:
            raise KeyError(f"could not find column {key}")
        return column[0]

    def __contains__(self, item):
        column = [column for column in self.columns if column.alias == item]
        if len(column):
            return True
        return False

    @property
    def properties(self):
        return ", ".join(
            [f"{column.term} {column.datatype}" for column in self.columns]
        )

    def _get_alias(self, term):
        if term == self.domain_schema.metric:
            return "metric"
        elif term == self.domain_schema.identifier:
            return "identifier"
        elif term == "timestamp":
            return "isotime"
        elif term == "date":
            return "isotime"
        else:
            raise ValueError(f"could not determine alias for column {term}")

    def _transform_datatype(self, datatype):
        if datatype == "integer":  # treat all integers as double for simplicity
            datatype = "double"
        elif datatype == "float":  # treat all floats as double for simplicity
            datatype = "double"
        elif datatype == "timestamp":
            datatype = "string"
        return datatype

    def _infer_datatype(self, alias):
        raise NotImplementedError("This should never be called for this type of model")
