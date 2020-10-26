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
import dataclasses
from dataclasses import dataclass, field
from datetime import datetime
from datetime import timezone
from typing import Any

from pyathena.async_cursor import AsyncCursor
from pyathena.result_set import AthenaResultSet


@dataclass
class Column:
    name: str
    type: type


class Columns(list):
    def __init__(self):
        self._columns = list()

    def append(self, item) -> None:
        self._columns.append(item)

    def __contains__(self, item):
        if isinstance(item, Column):
            return True if item in self._columns else False
        elif isinstance(item, dataclasses.Field):
            return True if Column(item.name, item.type) in self._columns else False


@dataclass
class QueryModel:
    result_set: AthenaResultSet = field(repr=False)
    results: list = field(init=False, default_factory=list, repr=False)
    fields: Columns = field(init=False, default_factory=Columns, repr=False)

    def __post_init__(self):
        for col_desc in self.result_set.description:
            col_name = col_desc[0]
            col_type = self._map_type(col_desc[1])
            column = Column(col_name, col_type)
            self.fields.append(column)

    def _map_type(self, type_str):
        if "varchar" in type_str:
            type = str
        else:
            raise NotImplementedError(f"unsupported type: {type_str}")
        return type

    def unpack(self):
        mro = self.__class__.__mro__
        if len(mro) != 3:
            raise NotImplementedError(
                "please subclass QueryModel as a dataclass to define your columns in order"
            )

        # get the fields to unpack from the query results
        parent_props = dataclasses.fields(mro[1])
        child_props = dataclasses.fields(mro[0])
        fields = [f for f in child_props if f not in parent_props]

        for row in self.result_set:
            for idx, field in enumerate(fields):
                setattr(self, field.name, field.type(row[idx]))
                yield self


@dataclass
class MaximumDate(QueryModel):
    max_date: str = field(init=False)

    @property
    def as_date(self):
        dt = datetime.strptime(self.max_date, "%Y-%m-%d")
        dt = dt.replace(tzinfo=timezone.utc)
        return dt


@dataclass
class Query:
    cursor: AsyncCursor
    query: str
    model: Any
    timeout: int = field(default=30)
    result_set: AthenaResultSet = field(init=False)
    query_id: str = field(init=False)

    def __post_init__(self):
        self.query_id, future = self.cursor.execute(self.query)
        self.result_set = self._validate_future(future)

    def _validate_future(self, future):
        """
        Validate the future and return the result - this might raise a TimeoutError or a ValueError if there was a
        problem with the request
        :param future: concurrent future
        :return: result
        """
        result_set = future.result(timeout=self.timeout)
        if result_set.state == "FAILED":
            raise ValueError(
                "failed athena %s request %s: %s"
                % (
                    result_set.statement_type,
                    result_set.query_id,
                    result_set.state_change_reason,
                )
            )

        return result_set

    @property
    def results(self):
        result_model = self.model(self.result_set)
        yield from result_model.unpack()
