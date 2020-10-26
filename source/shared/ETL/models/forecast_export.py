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
import re

from shared.Dataset.data_timestamp_format import DataTimestampFormat
from shared.ETL.models import Model, Dimension

QUANTILE_RE = re.compile(r"^[p|P]\d+")


class ForecastExportModel(Model):
    def _get_alias(self, term):
        if QUANTILE_RE.match(term):
            return term

        try:
            return super()._get_alias(term)
        except ValueError:
            return Dimension(term).alias

    def _infer_datatype(self, alias):
        datatype = "string"
        if alias == "metric":
            return "double"
        elif QUANTILE_RE.match(alias):
            return "double"
        return datatype

    def set_timestamp_format(self, ts_format: DataTimestampFormat):
        self["date"].expression = "to_iso8601(from_iso8601_timestamp(date)) AS isotime"
