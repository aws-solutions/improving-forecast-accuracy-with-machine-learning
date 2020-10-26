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
import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from time import sleep

import boto3
import pytest
from moto import mock_sts, mock_athena
from moto.athena import athena_backends
from moto.athena.responses import AthenaResponse

from shared.Dataset.dataset_file import DatasetFile
from shared.ETL.forecast_etl import (
    DatasetFileDataset,
    ForecastETL,
)
from shared.ETL.models import Dimension, Metadata
from shared.config import Config


def set_query_results(response=None):
    if not response:
        response = {
            "ResultSet": {"Rows": [], "ResultSetMetadata": {"ColumnInfo": []}},
            "UpdateCount": None,
        }

    setattr(AthenaResponse, "get_query_results", lambda self: json.dumps(response))


def set_job_lifecycle(cursor, region="us-east-1", wait=0.1, status="SUCCEEDED"):
    def update_jobs():
        sleep(wait)
        for execution_id, execution in athena_backends[region].executions.items():
            execution.status = status

    cursor._executor._initializer = update_jobs


@dataclass
class TriggeringKey:
    name: str
    key: str
    expects_timeseries: bool = field(default=True)
    expects_related: bool = field(default=False)
    expects_metadata: bool = field(default=False)

    @property
    def expected_datasets(self):
        return sum(
            1
            for item in [
                self.expects_timeseries,
                self.expects_related,
                self.expects_metadata,
            ]
            if item
        )


@pytest.fixture(
    params=[
        TriggeringKey("RetailDemandT", key="train/RetailDemandT.csv"),
        TriggeringKey(
            "RetailDemandTR", expects_related=True, key="train/RetailDemandTR.csv"
        ),
        TriggeringKey(
            "RetailDemandTR",
            expects_related=True,
            key="train/RetailDemandTR.related.csv",
        ),
        TriggeringKey(
            "RetailDemandTM", expects_metadata=True, key="train/RetailDemandTM.csv"
        ),
        TriggeringKey(
            "RetailDemandTM",
            expects_metadata=True,
            key="train/RetailDemandTM.metadata.csv",
        ),
        TriggeringKey(
            "RetailDemandTRM",
            expects_related=True,
            expects_metadata=True,
            key="train/RetailDemandTRM.csv",
        ),
        TriggeringKey(
            "RetailDemandTRM",
            expects_related=True,
            expects_metadata=True,
            key="train/RetailDemandTRM.related.csv",
        ),
        TriggeringKey(
            "RetailDemandTRM",
            expects_related=True,
            expects_metadata=True,
            key="train/RetailDemandTRM.metadata.csv",
        ),
    ],
    ids=[
        "RetailDemandT-From-T",
        "RetailDemandTR-From-T",
        "RetailDemandTR-From-R",
        "RetailDemandTM-From-T",
        "RetailDemandTM-From-M",
        "RetailDemandTRM-From-T",
        "RetailDemandTRM-From-R",
        "RetailDemandTRM-From-M",
    ],
)
def etl_forecast(request, sfn_configuration_data, s3_valid_files):
    """This represents all of the file uploads that might trigger a forecast"""
    config = Config.from_sfn(sfn_configuration_data)

    with mock_sts():
        dataset_file = DatasetFile(key=request.param.key, bucket="testbucket")
        forecast = config.forecast(dataset_file, request.param.key)
        yield (
            ForecastETL(
                workgroup="primary",
                schema="default",
                config=config,
                dataset_file=dataset_file,
                forecast=forecast,
            ),
            request.param,
        )


@pytest.fixture
def etl_forecast_trm(sfn_configuration_data, s3_valid_files):
    """This represents a single file upload"""
    config = Config.from_sfn(sfn_configuration_data)

    with mock_sts():
        dataset_file = DatasetFile(key="train/RetailDemandTRM.csv", bucket="testbucket")
        forecast = config.forecast(dataset_file, "RetailDemandTRM")
        yield ForecastETL(
            workgroup="primary",
            schema="default",
            config=config,
            dataset_file=dataset_file,
            forecast=forecast,
        )


@pytest.fixture
def trm_export_info():
    return {
        "ForecastExportJobArn": "arn:aws:forecast:us-east-1:abcdefghijkl:forecast-export-job/forecast_2020_01_01_00_00_00/export_RetailDemandTRM_2000_01_01_00_00_00",
        "ForecastExportJobName": "export_RetailDemandTRM_2020_01_01_00_00_00",
        "Destination": {
            "S3Config": {
                "Path": "s3://testbucket/exports/export_RetailDemandTRM_2000_01_01_00_00_00",
                "RoleArn": "arn:aws:iam::abcdefghijkl:role/forecast-stack-ForecastS3AccessRole-1P8WHOZMOJY5",
            }
        },
        "Status": "ACTIVE",
        "CreationTime": datetime(2020, 1, 1),
        "LastModificationTime": datetime(2020, 1, 1),
    }


@pytest.fixture(autouse=True)
def reset_metadata_and_dimension_counters():
    Dimension.dimension_number = 1
    Metadata.metadata_number = 1


def test_etl_get_datasets(etl_forecast):
    etl, triggering_key = etl_forecast

    ts, rts, md = etl._get_datasets()

    assert isinstance(ts, DatasetFileDataset)
    if triggering_key.expects_related:
        assert isinstance(rts, DatasetFileDataset)
    else:
        assert not rts

    if triggering_key.expects_metadata:
        assert isinstance(md, DatasetFileDataset)
    else:
        assert not md


@mock_athena
def test_cleanup_temp_tables(etl_forecast, caplog):
    # cleaning up a table has no result
    set_query_results()
    caplog.set_level(logging.INFO)

    with mock_athena():
        cli = boto3.client("athena", region_name="us-east-1")
        cli.create_work_group(Name="primary")

        etl, _ = etl_forecast
        set_job_lifecycle(etl.cursor)
        fs = etl.cleanup_temp_tables()

        assert (
            f"removing table {etl.unique_id} in workgroup {etl.workgroup} if it exists"
            in caplog.messages
        )


def test_copy_tts(etl_forecast_trm):
    result = etl_forecast_trm._copy_dataset(etl_forecast_trm.target_time_series)
    assert (
        result.dataset_file.s3_url
        == f"s3://testbucket/raw/{etl_forecast_trm.unique_id}/TARGET_TIME_SERIES/RetailDemandTRM.csv"
    )


def test_copy_rts(etl_forecast_trm):
    result = etl_forecast_trm._copy_dataset(etl_forecast_trm.related_time_series)
    assert (
        result.dataset_file.s3_url
        == f"s3://testbucket/raw/{etl_forecast_trm.unique_id}/RELATED_TIME_SERIES/RetailDemandTRM.related.csv"
    )


def test_copy_md(etl_forecast_trm):
    result = etl_forecast_trm._copy_dataset(etl_forecast_trm.item_metadata)
    assert (
        result.dataset_file.s3_url
        == f"s3://testbucket/raw/{etl_forecast_trm.unique_id}/ITEM_METADATA/RetailDemandTRM.metadata.csv"
    )


def test_input_table_name(etl_forecast):
    etl, _ = etl_forecast

    if etl.target_time_series:
        assert (
            etl.input_table_name(etl.target_time_series)
            == f"{etl.schema}.{etl.unique_id}"
        )
    if etl.related_time_series:
        assert (
            etl.input_table_name(etl.related_time_series)
            == f"{etl.schema}.{etl.unique_id}_related"
        )
    if etl.item_metadata:
        assert (
            etl.input_table_name(etl.item_metadata)
            == f"{etl.schema}.{etl.unique_id}_metadata"
        )


def test_input_table_name(etl_forecast):
    etl, triggering_key = etl_forecast

    table_names = etl.input_table_names()
    assert len(table_names) == triggering_key.expected_datasets


@mock_athena
def test_create_input_tables(etl_forecast, caplog, trm_export_info):
    # force all query results responses to look like an empty dictionary
    set_query_results()
    caplog.set_level(logging.DEBUG)

    with mock_athena():
        cli = boto3.client("athena", region_name="us-east-1")
        cli.create_work_group(Name="primary")

        etl, _ = etl_forecast
        etl.forecast.export_history = lambda: [trm_export_info, {}]
        set_job_lifecycle(etl.cursor)

        etl.create_input_tables()

        assert f"table {etl.unique_id} is creating" in caplog.messages
        if etl.item_metadata:
            assert f"table {etl.unique_id}_metadata is creating" in caplog.messages


def test_forecast_output_path_missing(etl_forecast_trm):
    etl_forecast_trm.forecast.export_history = lambda: []

    with pytest.raises(ValueError):
        etl_forecast_trm._get_forecast_export()


def test_forecast_output_path_available(etl_forecast_trm, trm_export_info):
    etl_forecast_trm.forecast.export_history = lambda: [trm_export_info, {}]
    assert (
        etl_forecast_trm._get_forecast_export().get("ForecastExportJobName")
        == "export_RetailDemandTRM_2020_01_01_00_00_00"
    )


def test_forecast_output_path_present(etl_forecast_trm, trm_export_info):
    (bucket, key) = etl_forecast_trm._get_export_path(trm_export_info)
    assert bucket == "testbucket"
    assert key == "exports/export_RetailDemandTRM_2000_01_01_00_00_00/some_file.csv"


def test_forecast_output_path_present(etl_forecast_trm, trm_export_info):
    (bucket, key) = etl_forecast_trm._get_export_path(trm_export_info)
    columns = etl_forecast_trm._get_export_columns(bucket, key)
    assert columns == ["item_id", "date", "location", "p10", "p50", "p90"]


def test_forecast_output_path_present(etl_forecast_trm, trm_export_info):
    trm_export_info["Destination"] = {}
    with pytest.raises(ValueError):
        etl_forecast_trm._get_export_path(trm_export_info)


def test_consolidate_data(etl_forecast_trm, trm_export_info, caplog):
    etl_forecast_trm.config.data_timestamp_format = lambda _: "yyyy-MM-dd HH:mm:ss"
    etl_forecast_trm._earliest_date = lambda: datetime(2007, 1, 1, tzinfo=timezone.utc)

    set_query_results()
    caplog.set_level(logging.INFO)

    with mock_athena():
        cli = boto3.client("athena", region_name="us-east-1")
        cli.create_work_group(Name="primary")

        etl_forecast_trm.forecast.export_history = lambda: [trm_export_info, {}]
        set_job_lifecycle(etl_forecast_trm.cursor)

        etl_forecast_trm.consolidate_data()


def test_dimension_count():
    d1 = Dimension("test1")
    d2 = Dimension("test2")

    assert d1.dimension_number == 1
    assert d2.dimension_number == 2


def test_dimension_count2():
    d1 = Dimension("test")
    d2 = Dimension("test")

    assert d1.dimension_number == 1
    assert d2.dimension_number == 1


def test_min_date(etl_forecast_trm):  # aws_credentials, sfn_configuration_data
    results = {
        "ResultSet": {
            "Rows": [{"Data": [{"VarCharValue": "2015-01-01"}]}],
            "ResultSetMetadata": {
                "ColumnInfo": [
                    {
                        "CatalogName": "hive",
                        "SchemaName": "",
                        "TableName": "",
                        "Name": "max_date",
                        "Label": "max_date",
                        "Type": "varchar",
                        "Precision": 2147483647,
                        "Scale": 0,
                        "Nullable": "UNKNOWN",
                        "CaseSensitive": True,
                    }
                ]
            },
        },
        "UpdateCount": None,
    }
    set_query_results(results)

    with mock_athena():
        cli = boto3.client("athena", region_name="us-east-1")
        cli.create_work_group(Name="primary")

        set_job_lifecycle(etl_forecast_trm.cursor)
        result = etl_forecast_trm._earliest_date()

        assert result == datetime(2007, 1, 1, 0, 0, tzinfo=timezone.utc)
