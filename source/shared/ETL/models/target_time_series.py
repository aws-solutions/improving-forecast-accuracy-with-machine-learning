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
from shared.Dataset.data_timestamp_format import DataTimestampFormat
from shared.ETL.models import Model, Dimension


class TargetTimeSeriesModel(Model):
    """Any unmapped columns are forecast metadata"""

    def _get_alias(self, term):
        try:
            return super()._get_alias(term)
        except ValueError:
            return Dimension(term).alias

    def set_timestamp_format(self, ts_format: DataTimestampFormat):
        if ts_format == "yyyy-MM-dd HH:mm:ss":
            ts_format = "%Y-%m-%d %H:%i:%s"
        elif ts_format == "yyyy-MM-dd":
            ts_format = "%Y-%m-%d"
        else:
            raise ValueError(f"Unexpected timestamp format: {ts_format}")

        self[
            "timestamp"
        ].expression = f"to_iso8601(date_parse(timestamp, '{ts_format}')) AS isotime"
