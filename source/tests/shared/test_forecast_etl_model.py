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
import pytest
from moto import mock_sts

from shared.Dataset.dataset_domain import DatasetDomain
from shared.DatasetGroup.dataset_group import DatasetGroup
from shared.DatasetGroup.dataset_group_name import DatasetGroupName
from shared.ETL.models import Dimension, Metadata
from shared.ETL.models.forecast_export import ForecastExportModel
from shared.ETL.models.target_time_series import TargetTimeSeriesModel


@pytest.fixture(autouse=True)
def reset_metadata_and_dimension_counters():
    Dimension.dimension_number = 1
    Metadata.metadata_number = 1


@pytest.fixture(
    params=[
        DatasetDomain.RETAIL,
        DatasetDomain.WORK_FORCE,
        DatasetDomain.WEB_TRAFFIC,
        DatasetDomain.CUSTOM,
        DatasetDomain.EC2_CAPACITY,
        DatasetDomain.INVENTORY_PLANNING,
        DatasetDomain.METRICS,
    ]
)
def schema(request):
    with mock_sts():
        dsg = DatasetGroup(DatasetGroupName("any"), request.param)
        dsg_schema = dsg.schema
        user_attributes = []

        for field in dsg_schema.fields:
            if field == dsg_schema.metric:
                typ = "float"
            elif field == dsg_schema.identifier:
                typ = "string"
            elif field == "timestamp":
                typ = "timestamp"
            else:
                raise ValueError(
                    f"Could not prepare attributes! Unexpected attribute {field}"
                )
            user_attributes.append({"AttributeName": field, "AttributeType": typ})

        yield dsg_schema, user_attributes


def test_schemas(schema):
    domain_schema, user_attributes = schema
    model = TargetTimeSeriesModel(domain_schema, user_attributes)

    assert len(model.columns) == 3
    has_identifier = False
    has_timestamp = False
    has_metric = False

    for column in model.columns:
        if column.alias == "identifier":
            has_identifier = True
        if column.alias == "isotime":
            has_timestamp = True
        if column.alias == "metric":
            has_metric = True

    assert all([has_identifier, has_timestamp, has_metric])


def test_schemas_dimensions(schema):
    domain_schema, user_attributes = schema
    user_attributes.extend(
        [
            {"AttributeName": f"userdefined_{n}", "AttributeType": "string"}
            for n in range(1, 11)
        ]
    )
    model = TargetTimeSeriesModel(domain_schema, user_attributes)
    exportmodel = ForecastExportModel(
        domain_schema,
        [attr.get("AttributeName") for attr in user_attributes] + ["p10", "p50", "p90"],
    )

    assert len(model.columns) == 13
    has_identifier = False
    has_timestamp = False
    has_metric = False

    for column in model.columns:
        if column.alias == "identifier":
            has_identifier = True
        if column.alias == "isotime":
            has_timestamp = True
        if column.alias == "metric":
            has_metric = True

    assert all([has_identifier, has_timestamp, has_metric])
