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
import os
from collections import namedtuple
from dataclasses import dataclass, field
from datetime import datetime
from math import nan, isnan
from tempfile import NamedTemporaryFile
from typing import Union

import boto3
import pytest
from moto.s3 import mock_s3

Schema = pytest.importorskip("glue.jobs.forecast_etl").Schema
Forecast = pytest.importorskip("glue.jobs.forecast_etl").ForecastStatus
ETL = pytest.importorskip("glue.jobs.forecast_etl").ETL
ForecastDataTransformation = pytest.importorskip(
    "glue.jobs.forecast_etl"
).ForecastDataTransformation

from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StringType, StructField

from awsglue import DynamicFrame
from awsglue.context import GlueContext

DOMAINS = [
    "RETAIL",
    "CUSTOM",
    "INVENTORY_PLANNING",
    "EC2_CAPACITY",
    "WORK_FORCE",
    "METRICS",
]
TARGET_FIELDS = {
    "RETAIL": "demand",
    "CUSTOM": "target_value",
    "INVENTORY_PLANNING": "demand",
    "EC2_CAPACITY": "number_of_instances",
    "WORK_FORCE": "workforce_demand",
    "WEB_TRAFFIC": "value",
    "METRICS": "metric_value",
}
IDENTIFIERS = {
    "RETAIL": "item_id",
    "CUSTOM": "item_id",
    "INVENTORY_PLANNING": "item_id",
    "EC2_CAPACITY": "instance_type",
    "WORK_FORCE": "workforce_type",
    "WEB_TRAFFIC": "item_id",
    "METRICS": "metric_name",
}

SC: Union[SparkContext, None] = None
GC: Union[GlueContext, None] = None
SPARK_HOME_DIR = spark_home_path = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        ".glue",
        "spark-2.4.3-bin-spark-2.4.3-bin-hadoop2.8",
    )
)
SPARK_CONF_DIR = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        os.pardir,
        os.pardir,
        os.pardir,
        ".glue",
        "aws-glue-libs-glue-1.0",
        "conf",
    )
)


def glue_not_available():
    global SC
    global GC

    if SC and GC:
        return False

    if not os.path.exists(SPARK_HOME_DIR) or not os.path.exists(SPARK_CONF_DIR):
        return True

    # configuration found - try to set up Glue
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["SPARK_CONF_DIR"] = SPARK_CONF_DIR
    os.environ["SPARK_HOME"] = SPARK_HOME_DIR
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python"

    SC = SparkContext.getOrCreate()
    GC = GlueContext(SC)
    return False


def validate_data(data, expected_header, expected_values):
    if isinstance(data, ETL):
        data = data.df.toDF()
    elif isinstance(data, DynamicFrame):
        data = data.toDF()
    elif isinstance(data, DataFrame):
        pass  # already a pyspark DataFrame

    data = data.toPandas()
    columns = data.columns.to_list()
    assert columns == expected_header

    expected_rows = len(expected_values)
    expected_columns = len(expected_values[0])
    rows, columns = data.shape

    assert expected_rows == rows
    assert expected_columns == columns

    for row_idx, row in enumerate(data.values):
        for col_idx, val in enumerate(row):
            val_expected = expected_values[row_idx][col_idx]
            if isinstance(val_expected, float) and isnan(val_expected):
                assert isnan(val)
            else:
                assert val_expected == val


@pytest.fixture
def tts_data_sample():
    content = "item_id,timestamp,target_value"  # header might not exist
    content += "\nitem_01,2000-01-01 01:01:01,1"
    content += "\nitem_01,2000-01-02 02:02:02,2"
    content += "\nitem_01,2000-01-03 03:03:03,3"
    content += "\n"  # bad data - empty line
    content += "\nitem_01,2000-01-04 04:04:04,4"
    content += "\nitem_01,2000-01-05 05:05:05,5"
    content += "\nitem_01,01:01:01,5"  # bad data - bad timestamp
    content += "\nitem_01,2000-01-06 06:06:06,6"
    content += "\n,,,,,,,,,,bad_data"  # bad data, too many fields
    content += "\n,"  # bad data, not enough fields
    content += "\n,,"  # bad data, enough fields, null fields
    content += "\n,2000-01-01 01:01:01,5"  # bad data, missing identifier
    content += "\nitem_01,,5"  # bad data, missing timestamp
    content += (
        "\nitem_01,2000-01-07 07:07:07,"  # not bad data - this is a legitimate null
    )
    content += "\n\n\n"  # some extra empty lines at the end
    return content


@pytest.fixture
def md_data_sample():
    content = "item_id,geo,store_id"
    content += "\nitem_01,CA,store_01"
    content += "\nitem_02,CA,store_02"
    content += "\n\n"
    content += "\n,CA,store_02"  # bad data missing identifier
    content += "\nitem_03,CA,"  # bad data missing a metadata field
    content += "\nitem_04,,store_03"  # bad data missing a geo
    return content


@pytest.fixture
def forecast_export_sample():
    content = "item_id,date,p10,p50,p90"
    content += "\nitem_01,2020-07-01T00:00:00Z,1.0,2.0,3.0"
    return content


@pytest.fixture
def predictor_backtest_sample():
    content = "item_id,timestamp,target_value,backtestwindow_start_time,backtestwindow_end_time,p10,p50,p90"
    content += "\nitem_01,2019-07-14T12:38:26Z,0.0,2019-07-14T12:38:26,2020-06-14T12:38:26,1.0,2.0,3.0"
    return content


@pytest.fixture(params=DOMAINS)
def dataset_by_domain(request, mocker):
    dataset = mocker.MagicMock()
    fields = [
        "timestamp",
        TARGET_FIELDS[request.param],
        IDENTIFIERS[request.param],
        "geolocation",
    ]
    field_type = ["timestamp", "float", "string", "geolocation"]
    type(dataset).Schema = mocker.PropertyMock(
        return_value={
            "Attributes": [
                {"AttributeName": field, "AttributeType": field_type[idx]}
                for idx, field in enumerate(fields)
            ]
        }
    )
    return (dataset, request.param)


@pytest.fixture(params=DOMAINS)
def dynamic_frame_schema_by_domain(request):
    schema = StructType(
        [
            StructField("timestamp", StringType(), True),
            StructField("geolocation", StringType(), True),
            StructField(TARGET_FIELDS[request.param], StringType(), True),
            StructField(IDENTIFIERS[request.param], StringType(), True),
        ]
    )
    return GC.createDataFrame(SC.emptyRDD(), schema), request.param


@pytest.fixture
def forecast_service_mock(mocker):
    real_s3_cli = boto3.client("s3", region_name="us-east-1")
    cli = mocker.patch("boto3.client")
    cli().describe_dataset_group.return_value = {
        "CreationTime": datetime.now(),
        "DatasetArns": ["arn:aws:forecast:us-east-1:abcdefghijkl:dataset/test"],
        "DatasetGroupArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/test",
        "DatasetGroupName": "test",
        "Domain": "CUSTOM",
        "LastModificationTime": datetime.now(),
        "Status": "ACTIVE",
    }
    cli().describe_dataset.return_value = {
        "CreationTime": datetime.now(),
        "DataFrequency": "W",
        "DatasetArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset/test",
        "DatasetName": "test",
        "DatasetType": "TARGET_TIME_SERIES",
        "Domain": "CUSTOM",
        "LastModificationTime": datetime.now(),
        "Schema": {
            "Attributes": [
                {"AttributeName": "item_id", "AttributeType": "string"},
                {"AttributeName": "timestamp", "AttributeType": "timestamp"},
                {"AttributeName": "target_value", "AttributeType": "float"},
            ]
        },
        "Status": "ACTIVE",
    }
    cli().describe_dataset_import_job = {
        "CreationTime": datetime.now(),
        "DatasetArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset/test",
        "DatasetImportJobArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-import-job/test-2000_01_01_01_01_01",
        "DatasetImportJobName": "test-2000_01_01_01_01_01",
        "DataSize": datetime.now(),
        "DataSource": {
            "S3Config": {
                "Path": "s3://data-bucket/train.csv",
                "RoleArn": "arn:aws:iam:us-east-1:abcdefghijkl:/role/some-role",
            }
        },
        "FieldStatistics": {
            "string": {
                "Avg": 1,
                "Count": 2,
                "CountDistinct": 2,
                "CountNan": 0,
                "CountNull": 0,
                "Max": "2",
                "Min": "1",
                "Stddev": 0.5,
            }
        },
        "LastModificationTime": datetime.now,
        "Status": "ACTIVE",
    }
    cli().describe_predictor.return_value = {
        "AlgorithmArn": "arn:aws:forecast:::algorithm/ARIMA",
        "CreationTime": datetime.now(),
        "DatasetImportJobArns": [
            "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-import-job/test-2000_01_01_01_01_01"
        ],
        "EvaluationParameters": {
            "BackTestWindowOffset": 3,
            "NumberOfBacktestWindows": 3,
        },
        "FeaturizationConfig": {"ForecastFrequency": "W"},
        "ForecastHorizon": 30,
        "ForecastTypes": ["0.10", "mean", "0.90"],
        "InputDataConfig": {
            "DatasetGroupArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/test",
            "SupplementaryFeatures": [{"Name": "holiday", "Value": "CA"}],
        },
        "LastModificationTime": datetime.now(),
        "PredictorArn": "arn:aws:forecast:us-east-1:abcdefghijkl:predictor/test_2000_01_01_01_01_01",
        "PredictorName": "test_2000_01_01_01_01_01",
        "Status": "ACTIVE",
    }
    cli().get_paginator.return_value.paginate.return_value = [
        {
            "Forecasts": [
                {
                    "CreationTime": datetime.now(),
                    "DatasetGroupArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/test",
                    "ForecastArn": "arn:aws:forecast:us-east-1:abcdefghijkl:forecast/test_2000_01_01_01_01_01",
                    "ForecastName": "test_2000_01_01_01_01_01",
                    "PredictorArn": "arn:aws:forecast:us-east-1:abcdefghijkl:predictor/test_2000_01_01_01_01_01",
                    "Status": "ACTIVE",
                }
            ],
            "Predictors": [
                {
                    "CreationTime": datetime.now(),
                    "DatasetGroupArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/test",
                    "LastModificationTime": datetime.now(),
                    "PredictorArn": "arn:aws:forecast:us-east-1:abcdefghijkl:predictor/test_2000_01_01_01_01_01",
                    "PredictorName": "test_2000_01_01_01_01_01",
                    "Status": "ACTIVE",
                }
            ],
            "PredictorBacktestExportJobs": [
                {
                    "CreationTime": datetime.now(),
                    "Destination": {
                        "S3Config": {
                            "KMSKeyArn": "string",
                            "Path": "s3://predictor-backtest-export/job",
                            "RoleArn": "arn:aws:iam:us-east-1:abcdefghijkl:/role/some-role",
                        }
                    },
                    "LastModificationTime": datetime.now(),
                    "PredictorBacktestExportJobArn": "arn:aws:forecast:us-east-1:abcdefghijkl:predictor-backtest-export-job/test_2000_01_01_01_01_01",
                    "PredictorBacktestExportJobName": "test_2000_01_01_01_01_01",
                    "Status": "ACTIVE",
                }
            ],
            "ForecastExportJobs": [
                {
                    "CreationTime": datetime.now(),
                    "Destination": {
                        "S3Config": {
                            "Path": "s3://forecast-export/job",
                            "RoleArn": "arn:aws:iam:us-east-1:abcdefghijkl:/role/some-role",
                        }
                    },
                    "ForecastExportJobArn": "arn:aws:forecast:us-east-1:abcdefghijkl:forecast-export-job/test_2000_01_01_01_01_01",
                    "ForecastExportJobName": "test_2000_01_01_01_01_01",
                    "LastModificationTime": datetime.now(),
                    "Message": "string",
                    "Status": "string",
                }
            ],
            "DatasetImportJobs": [
                {
                    "CreationTime": datetime.now(),
                    "DatasetImportJobArn": "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-import-job/test-2000_01_01_01_01_01",
                    "DatasetImportJobName": "test-2000_01_01_01_01_01",
                    "DataSource": {
                        "S3Config": {
                            "Path": "s3://data-bucket/train.csv",
                            "RoleArn": "arn:aws:iam:us-east-1:abcdefghijkl:/role/some-role",
                        }
                    },
                    "LastModificationTime": datetime.now(),
                    "Status": "ACTIVE",
                }
            ],
        }
    ]
    return cli


def test_schema_fields(dataset_by_domain):
    dataset, domain = dataset_by_domain
    schema = Schema(dataset, domain)

    assert "geolocation" in schema.fields
    assert "timestamp" in schema.fields
    assert IDENTIFIERS[domain] in schema.fields
    assert TARGET_FIELDS[domain] in schema.fields


@pytest.mark.skipif(glue_not_available(), reason="requires local glue environment")
def test_schema_mappings(dataset_by_domain):
    dataset, domain = dataset_by_domain
    schema = Schema(dataset, domain)

    df_schema = StructType(
        [
            StructField("timestamp", StringType(), True),
            StructField(TARGET_FIELDS[domain], StringType(), True),
            StructField(IDENTIFIERS[domain], StringType(), True),
            StructField("geolocation", StringType(), True),
        ]
    )

    df = DynamicFrame.fromDF(
        dataframe=GC.createDataFrame(SC.emptyRDD(), df_schema), glue_ctx=GC, name=domain
    )
    target_field = TARGET_FIELDS[domain]
    identifier = IDENTIFIERS[domain]

    assert ("`timestamp`", "string", "timestamp", "timestamp") in schema.mappings(df)
    assert (f"`{target_field}`", "string", target_field, "double") in schema.mappings(
        df
    )
    assert (f"`{identifier}`", "string", identifier, "string") in schema.mappings(df)
    assert ("`geolocation`", "string", "geolocation", "string") in schema.mappings(df)


def test_schema_attribute_repr():
    attr = Schema._Attribute("a", "b")
    assert repr(attr) == "_Attribute('a', 'b')"


def test_forecast_domain(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    assert fc.domain == "CUSTOM"


def test_forecast_target_field(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    assert fc.target_field == "target_value"


def test_forecast_identifier_field(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    assert fc.identifier == "item_id"


def test_forecast_dataset_group(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    assert (
        fc.dataset_group.DatasetGroupArn
        == "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-group/test"
    )


def test_forecast_forecast(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    forecast = fc.forecast
    assert (
        forecast.ForecastArn
        == "arn:aws:forecast:us-east-1:abcdefghijkl:forecast/test_2000_01_01_01_01_01"
    )


def test_forecast_predictor(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    predictor = fc.predictor
    assert (
        predictor.PredictorArn
        == "arn:aws:forecast:us-east-1:abcdefghijkl:predictor/test_2000_01_01_01_01_01"
    )


def test_s3_url(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    predictor_backtest_export_job = fc.predictor_backtest_export_job
    url = fc.s3_url(predictor_backtest_export_job)
    assert url == "s3://predictor-backtest-export/job"

    forecast_export_job = fc.forecast_export_job
    url = fc.s3_url(forecast_export_job)
    assert url == "s3://forecast-export/job"

    tts = fc._dataset_import_job("arn:aws:forecast:us-east-1:abcdefghijkl:dataset/test")
    url = fc.s3_url(tts)
    assert url == "s3://data-bucket/train.csv"


def test_target_time_series(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    tts = fc.target_time_series
    assert tts.DatasetArn == "arn:aws:forecast:us-east-1:abcdefghijkl:dataset/test"


def test_target_time_series_import_job(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    import_job = fc.target_time_series_import_job
    assert (
        import_job.DatasetImportJobArn
        == "arn:aws:forecast:us-east-1:abcdefghijkl:dataset-import-job/test-2000_01_01_01_01_01"
    )


def test_target_time_series_schema(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    schema = fc.target_time_series_schema
    assert schema.domain == "CUSTOM"
    assert schema.fields == ["item_id", "timestamp", "target_value"]


@mock_s3
@pytest.mark.skipif(glue_not_available(), reason="requires local glue environment")
def test_target_time_series_data(mocker, tts_data_sample, md_data_sample):
    bucket = "test-bucket"
    key_tts = "data.csv"
    key_md = "data.metadata.csv"

    cli = boto3.client("s3", region_name="us-east-1")
    cli.create_bucket(Bucket="test-bucket")
    cli.put_object(Bucket=bucket, Key=key_tts, Body=tts_data_sample)
    cli.put_object(Bucket=bucket, Key=key_md, Body=md_data_sample)

    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    type(fc).predictor = mocker.PropertyMock(
        return_value=namedtuple("Predictor", "FeaturizationConfig")(
            FeaturizationConfig={"ForecastDimensions": [], "ForecastFrequency": "W"}
        )
    )
    type(fc).domain = mocker.PropertyMock(return_value="CUSTOM")
    type(fc).target_time_series_import_job = mocker.PropertyMock(
        return_value=namedtuple("DatasetImportJob", "DataSource")(
            DataSource={"S3Config": {"Path": f"s3://{bucket}/{key_tts}"}}
        )
    )
    type(fc).item_metadata_import_job = mocker.PropertyMock(
        return_value=namedtuple("DatasetImportJob", "DataSource")(
            DataSource={"S3Config": {"Path": f"s3://{bucket}/{key_md}"}}
        )
    )
    type(fc).target_time_series = mocker.PropertyMock(
        return_value=namedtuple("Dataset", "Schema")(
            Schema={
                "Attributes": [
                    {"AttributeName": "item_id", "AttributeType": "string"},
                    {"AttributeName": "timestamp", "AttributeType": "timestamp"},
                    {"AttributeName": "target_value", "AttributeType": "integer"},
                ]
            }
        )
    )
    type(fc).item_metadata = mocker.PropertyMock(
        return_value=namedtuple("Dataset", "Schema")(
            Schema={
                "Attributes": [
                    {"AttributeName": "item_id", "AttributeType": "string"},
                    {"AttributeName": "geo", "AttributeType": "string"},
                    {"AttributeName": "store_id", "AttributeType": "string"},
                ]
            }
        )
    )

    # use named temporary files to load from disk
    with NamedTemporaryFile("w", suffix="", prefix="TARGET_TIME_SERIES") as tts_data:
        with NamedTemporaryFile("w", suffix="", prefix="ITEM_METADATA") as md_data:
            tts_data.write(tts_data_sample)
            tts_data.flush()

            md_data.write(md_data_sample)
            md_data.flush()

            # point the loader to the sample data
            mocker.patch("glue.jobs.forecast_etl.TARGET_TIME_SERIES", new=tts_data.name)
            mocker.patch("glue.jobs.forecast_etl.ITEM_METADATA", new=md_data.name)
            tts = fc.target_time_series_data
            md = fc.item_metadata_data

            tts_columns_expected = ["identifier", "timestamp", "metric"]
            tts_expected = [
                ["item_01", datetime(1999, 12, 27), 3],
                ["item_01", datetime(2000, 1, 3), 18],
            ]
            validate_data(tts, tts_columns_expected, tts_expected)

            md_columns_expected = ["identifier", "geo", "store_id"]
            md_expected = [["item_01", "CA", "store_01"], ["item_02", "CA", "store_02"]]
            validate_data(md, md_columns_expected, md_expected)


def test_related_time_series(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    rts = fc.related_time_series
    assert not rts


def test_related_time_series_import_job(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    with pytest.raises(AttributeError):
        import_job = fc.related_time_series_import_job


def test_related_time_series_schema(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    schema = fc.related_time_series_schema
    assert not schema


def test_related_time_series_data(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    with pytest.raises(AttributeError):
        data = fc.related_time_series_data


def test_item_metadata(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    md = fc.item_metadata
    assert not md


def test_item_metadata_import_job(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    with pytest.raises(AttributeError):
        import_job = fc.item_metadata_import_job


def test_item_metadata_schema(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    schema = fc.item_metadata_schema
    assert not schema


def test_item_metadata_series_data(forecast_service_mock):
    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    with pytest.raises(AttributeError):
        data = fc.item_metadata_data


@pytest.mark.skipif(glue_not_available(), reason="requires local glue environment")
def test_empty_output_schema():
    empty = Forecast.empty()
    fields = empty.schema.names

    required = [
        "identifier",
        "timestamp",
        "metric",
        *[f"p{n}" for n in range(1, 100)],
        "mean",
        *[f"dimension_{n}" for n in range(1, 11)],
        *[f"metadata_{n}" for n in range(1, 11)],
    ]
    for item in required:
        fields.remove(item)
    assert len(fields) == 0


@pytest.mark.skipif(glue_not_available(), reason="requires local glue environment")
@pytest.mark.parametrize(
    "df_a,df_b,expected_values,expected_columns",
    [
        (
            SC.parallelize([[1, 2, 3], [4, 5, 6]]).toDF(("a", "b", "c")),
            SC.parallelize([[7, 8, 9], [10, 11, 12]]).toDF(("c", "d", "a")),
            [[1, 2, 3, nan], [4, 5, 6, nan], [9, nan, 7, 8], [12, nan, 10, 11]],
            ["a", "b", "c", "d"],
        ),
        (
            SC.parallelize([[7, 8, 9], [10, 11, 12]]).toDF(("c", "d", "a")),
            SC.parallelize([[1, 2, 3], [4, 5, 6]]).toDF(("a", "b", "c")),
            [[7, 8, 9, nan], [10, 11, 12, nan], [3, nan, 1, 2], [6, nan, 4, 5]],
            ["c", "d", "a", "b"],
        ),
    ],
)
def test_union_code(df_a, df_b, expected_values, expected_columns):
    union = Forecast.union_dfs(df_a, df_b)
    validate_data(union, expected_columns, expected_values)


@mock_s3
@pytest.mark.skipif(glue_not_available(), reason="requires local glue environment")
def test_target_time_series_data(
    mocker,
    tts_data_sample,
    md_data_sample,
    predictor_backtest_sample,
    forecast_export_sample,
):
    bucket = "test-bucket"
    key_tts = "data.csv"
    key_md = "data.metadata.csv"
    key_predictor_backtest = "predictor"
    key_forecast_export = "forecast"

    cli = boto3.client("s3", region_name="us-east-1")
    cli.create_bucket(Bucket="test-bucket")
    cli.put_object(Bucket=bucket, Key=key_tts, Body=tts_data_sample)
    cli.put_object(Bucket=bucket, Key=key_md, Body=md_data_sample)
    cli.put_object(
        Bucket=bucket,
        Key=f"{key_predictor_backtest}/forecasted-values/sample.csv",
        Body=predictor_backtest_sample,
    )
    cli.put_object(
        Bucket=bucket,
        Key=f"{key_forecast_export}/sample.csv",
        Body=forecast_export_sample,
    )

    fc = Forecast("test", "us-east-1", "abcdefghijkl")
    type(fc).predictor = mocker.PropertyMock(
        return_value=namedtuple("Predictor", "FeaturizationConfig")(
            FeaturizationConfig={"ForecastDimensions": [], "ForecastFrequency": "W"}
        )
    )
    type(fc).domain = mocker.PropertyMock(return_value="CUSTOM")
    type(fc).target_time_series_import_job = mocker.PropertyMock(
        return_value=namedtuple("DatasetImportJob", "DataSource")(
            DataSource={"S3Config": {"Path": f"s3://{bucket}/{key_tts}"}}
        )
    )
    type(fc).item_metadata_import_job = mocker.PropertyMock(
        return_value=namedtuple("DatasetImportJob", "DataSource")(
            DataSource={"S3Config": {"Path": f"s3://{bucket}/{key_md}"}}
        )
    )
    type(fc).predictor_backtest_export_job = mocker.PropertyMock(
        return_value=namedtuple("PredictorBacktestExportJob", "Destination")(
            Destination={
                "S3Config": {"Path": f"s3://{bucket}/{key_predictor_backtest}"}
            }
        )
    )
    type(fc).forecast_export_job = mocker.PropertyMock(
        return_value=namedtuple("ForecastExportJob", "Destination")(
            Destination={"S3Config": {"Path": f"s3://{bucket}/{key_forecast_export}"}}
        )
    )
    type(fc).target_time_series = mocker.PropertyMock(
        return_value=namedtuple("Dataset", "Schema")(
            Schema={
                "Attributes": [
                    {"AttributeName": "item_id", "AttributeType": "string"},
                    {"AttributeName": "timestamp", "AttributeType": "timestamp"},
                    {"AttributeName": "target_value", "AttributeType": "integer"},
                ]
            }
        )
    )
    type(fc).item_metadata = mocker.PropertyMock(
        return_value=namedtuple("Dataset", "Schema")(
            Schema={
                "Attributes": [
                    {"AttributeName": "item_id", "AttributeType": "string"},
                    {"AttributeName": "geo", "AttributeType": "string"},
                    {"AttributeName": "store_id", "AttributeType": "string"},
                ]
            }
        )
    )

    # use named temporary files to load from disk
    with NamedTemporaryFile("w", suffix="", prefix="TARGET_TIME_SERIES") as tts_data:
        with NamedTemporaryFile("w", suffix="", prefix="ITEM_METADATA") as md_data:
            with NamedTemporaryFile(
                "w", suffix="", prefix="PREDICTOR_BACKTEST_EXPORT_JOB"
            ) as predictor_data:
                with NamedTemporaryFile(
                    "w", suffix="", prefix="FORECAST_EXPORT_JOB"
                ) as forecast_data:
                    tts_data.write(tts_data_sample)
                    tts_data.flush()

                    md_data.write(md_data_sample)
                    md_data.flush()

                    predictor_data.write(predictor_backtest_sample)
                    predictor_data.flush()

                    forecast_data.write(forecast_export_sample)
                    forecast_data.flush()

                    # point the loader to the sample data
                    mocker.patch(
                        "glue.jobs.forecast_etl.TARGET_TIME_SERIES", new=tts_data.name
                    )
                    mocker.patch(
                        "glue.jobs.forecast_etl.ITEM_METADATA", new=md_data.name
                    )
                    mocker.patch(
                        "glue.jobs.forecast_etl.PREDICTOR_BACKTEST_EXPORT_JOB",
                        new=predictor_data.name,
                    )
                    mocker.patch(
                        "glue.jobs.forecast_etl.FORECAST_EXPORT_JOB",
                        new=forecast_data.name,
                    )

                    # have spark load the time series data
                    tts = fc.target_time_series_data
                    md = fc.item_metadata_data
                    forecast = fc.forecast_export_job_data
                    predictor = fc.predictor_backtest_export_job_data

                    # test the individual time series are processed as expected
                    tts_columns_expected = ["identifier", "timestamp", "metric"]
                    tts_expected = [
                        ["item_01", datetime(1999, 12, 27), 3],
                        ["item_01", datetime(2000, 1, 3), 18],
                    ]
                    validate_data(tts, tts_columns_expected, tts_expected)

                    md_columns_expected = ["identifier", "geo", "store_id"]
                    md_expected = [
                        ["item_01", "CA", "store_01"],
                        ["item_02", "CA", "store_02"],
                    ]
                    validate_data(md, md_columns_expected, md_expected)

                    forecast_columns_expected = [
                        "identifier",
                        "timestamp",
                        "p10",
                        "p50",
                        "p90",
                    ]
                    forecast_expected = [
                        ["item_01", datetime(2020, 7, 1), 1.0, 2.0, 3.0]
                    ]
                    validate_data(
                        forecast, forecast_columns_expected, forecast_expected
                    )

                    predictor_columns_expected = [
                        "identifier",
                        "timestamp",
                        "metric",
                        "p10",
                        "p50",
                        "p90",
                    ]
                    predictor_expected = [
                        [
                            "item_01",
                            datetime(2019, 7, 14, 12, 38, 26),
                            0.0,
                            1.0,
                            2.0,
                            3.0,
                        ]
                    ]
                    validate_data(
                        predictor, predictor_columns_expected, predictor_expected
                    )

                    # finally aggregate the data
                    result = fc.aggregate_forecast_data.toDF().toPandas()
                    assert all(
                        identifier == "item_01"
                        for identifier in result["identifier"].to_list()
                    )
                    assert all(geo == "CA" for geo in result["metadata_1"].to_list())
                    assert all(
                        store_id == "store_01"
                        for store_id in result["metadata_2"].to_list()
                    )
                    assert result["metric"].to_list()[:3] == [3, 18, 0]
                    assert len(result) == 4


@pytest.fixture
def etl_mock():
    @dataclass
    class MockETL:
        df: Union[DataFrame, DynamicFrame, None] = field(default=None, repr=False)
        name: str = field(default="SomeName")
        gc: GlueContext = field(default=GC)
        sc: SparkContext = field(default=SC)

        def set_df(self, dataframe):
            self.df = DynamicFrame.fromDF(dataframe, GC, self.name)

    return MockETL()


@pytest.mark.skipif(glue_not_available(), reason="requires local glue environment")
def test_forecast_data_transformation_drop_duplicates(etl_mock):
    etl_mock.set_df(SC.parallelize([[1, 2, 3], [1, 2, 3]]).toDF(("a", "b", "c")))
    fdt = ForecastDataTransformation(etl_mock)
    fdt.drop_duplicates()

    validate_data(fdt.etl.df, ["a", "b", "c"], [[1, 2, 3]])
