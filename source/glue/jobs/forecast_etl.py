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

from __future__ import annotations

import csv
import logging
import re
import sys
from collections import namedtuple
from functools import lru_cache, wraps
from typing import List, Union, Tuple
from urllib.parse import urlparse

import boto3
import pyspark.sql.functions as F
from botocore.config import Config
from pyspark import SparkContext
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructField,
    StringType,
    DoubleType,
    TimestampType,
    StructType,
)
from pyspark.sql.utils import AnalysisException

from awsglue import DynamicFrame
from awsglue.context import GlueContext
from awsglue.transforms import (
    DropNullFields,
    ApplyMapping,
    Filter,
    SelectFields,
    RenameField,
    DropFields,
)
from awsglue.utils import getResolvedOptions

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s %(levelname)s %(name)s: %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
)
logger.addHandler(handler)


QUANTILE_RE = re.compile(r"^[p|P]\d+$|^mean$")
METADATA_RE = re.compile(r"^metadata_\d+$")
TIME_RE = r"^\d{4}-\d{1,2}-\d{1,2}$|^\d{4}-\d{1,2}-\d{1,2} \d{1,2}:\d{1,2}:\d{1,2}$"
TARGET_TIME_SERIES = "TARGET_TIME_SERIES"
RELATED_TIME_SERIES = "RELATED_TIME_SERIES"
ITEM_METADATA = "ITEM_METADATA"
FORECAST_EXPORT_JOB = "FORECAST_EXPORT_JOB"
PREDICTOR_BACKTEST_EXPORT_JOB = "PREDICTOR_BACKTEST_EXPORT_JOB"
SOLUTION_ID = "SO0123"
SOLUTION_VERSION = "1.3.1"
CLIENT_CONFIG = Config(
    retries={"max_attempts": 10, "mode": "standard"},
    user_agent_extra=f"AwsSolution/{SOLUTION_ID}/{SOLUTION_VERSION}",
)


class Schema:
    """Hold information about an Amazon Forecast Schema"""

    class _Attribute:
        """A schema consists of attributes - AttributeName/ AttributeType (name/ kind)"""

        def __init__(self, name: str, kind: str):
            """
            :param name: The name of the attribute (AttributeName)
            :param kind: The type of the attribute (AttributeType)
            """
            self.name = name
            self.kind = self._lookup_kind(kind)

        def _lookup_kind(self, kind: str) -> str:
            """Map attribute kinds to data types that can be understood by spark and are nullable"""
            if kind == "geolocation":
                return "string"
            if kind == "integer":
                return "int"
            if kind == "float":
                return "double"
            else:
                return kind

        def __repr__(self):
            return f"_Attribute('{self.name}', '{self.kind}')"

    def __init__(self, dataset, domain):
        """
        Initialize the Schema representation for the dataset and domain
        :param dataset: The dataset (namedtuple)
        :param domain: The domain of the dataset
        """
        self.domain = domain
        self.attributes = [
            self._Attribute(
                attribute.get("AttributeName"), attribute.get("AttributeType")
            )
            for attribute in dataset.Schema.get("Attributes", [])
        ]

    def mappings(self, dynamic_frame) -> List[Tuple[str, str, str, str]]:
        """Get a mapping for the attributes in this dataset compatible with ApplyMapping transforms"""
        return [
            (
                f"`{dynamic_frame.schema().fields[idx].name}`",
                "string",
                attribute.name,
                attribute.kind,
            )
            for idx, attribute in enumerate(self.attributes)
        ]

    @property
    def fields(self) -> List[str]:
        """
        :return: The list of fields in this schema
        """
        return [attribute.name for attribute in self.attributes]


class ForecastStatus:
    """Used to cache information about the current state of the Forecast service"""

    def __init__(self, name, region, account):
        """
        Initialize the Forecast Service
        :param name: the name of the dataset group to inspect the status of
        :param region: region for forecast service
        :param account: string AWS account ID
        """
        self.cli = boto3.client("forecast", region_name=region, config=CLIENT_CONFIG)
        self.region = region
        self.account = account
        self.name = name

    @property
    @lru_cache()
    def domain(self) -> str:
        """The dataset and dataset group domain"""
        return self.dataset_group.Domain

    @property
    @lru_cache()
    def target_field(self) -> str:
        """
        :return: The target field ("metric") for the dataset domain
        """
        target_fields = {
            "RETAIL": "demand",
            "CUSTOM": "target_value",
            "INVENTORY_PLANNING": "demand",
            "EC2_CAPACITY": "number_of_instances",
            "WORK_FORCE": "workforce_demand",
            "WEB_TRAFFIC": "value",
            "METRICS": "metric_value",
        }
        return target_fields[self.domain]

    @property
    @lru_cache()
    def identifier(self) -> str:
        """
        :return: The identifier (thing to forecast) for the dataset domain
        """
        identifiers = {
            "RETAIL": "item_id",
            "CUSTOM": "item_id",
            "INVENTORY_PLANNING": "item_id",
            "EC2_CAPACITY": "instance_type",
            "WORK_FORCE": "workforce_type",
            "WEB_TRAFFIC": "item_id",
            "METRICS": "metric_name",
        }
        return identifiers[self.domain]

    @property
    @lru_cache()
    def dataset_group(self):
        """
        :return: The dataset group (as a named tuple)
        """
        dsg = self.cli.describe_dataset_group(
            DatasetGroupArn=f"arn:aws:forecast:{self.region}:{self.account}:dataset-group/{self.name}"
        )
        return namedtuple("DatasetGroup", dsg)(**dsg)

    @property
    @lru_cache()
    def target_time_series(self):
        """
        :return: The target time series dataset info (from describe_dataset)
        """
        return self._dataset(TARGET_TIME_SERIES)

    @property
    @lru_cache()
    def target_time_series_import_job(self):
        """
        :return: The target time series dataset import job (from list_dataset_import_jobs)
        """
        return self._dataset_import_job(self.target_time_series.DatasetArn)

    @property
    @lru_cache()
    def target_time_series_schema(self) -> Schema:
        """
        :return: The target time series Schema
        """
        return self._schema(self.target_time_series)

    @property
    @lru_cache()
    def target_time_series_data(self):
        """
        :return: The cleaned and binned target time series data as an ETL object
        """
        dimensions = self.predictor.FeaturizationConfig.get("ForecastDimensions", [])
        frequency = self.predictor.FeaturizationConfig["ForecastFrequency"]
        etl = ETL(
            name=TARGET_TIME_SERIES,
            schema=self.target_time_series_schema,
            source=self.s3_url(self.target_time_series_import_job),
            identifier=self.identifier,
            target_field=self.target_field,
        )
        transformer = ForecastInputTransformation(etl)
        transformer.apply()
        transformer.apply_bucketing(dimensions, frequency)
        return etl

    @property
    @lru_cache()
    def related_time_series(self):
        """
        :return: The related time series dataset info (from describe_dataset)
        """
        return self._dataset(RELATED_TIME_SERIES)

    @property
    @lru_cache()
    def related_time_series_import_job(self):
        """
        :return: The related time series dataset import job info (from list_dataset_import_job)
        """
        return self._dataset_import_job(self.related_time_series.DatasetArn)

    @property
    @lru_cache()
    def related_time_series_schema(self) -> Schema:
        """
        :return: The related time series Schema
        """
        return self._schema(self.related_time_series)

    @property
    @lru_cache()
    def related_time_series_data(self) -> ETL:
        """
        :return: The cleaned related time series data as an ETL object
        """
        return ETL(
            name=RELATED_TIME_SERIES,
            schema=self.related_time_series_schema,
            source=self.s3_url(self.related_time_series_import_job),
            identifier=self.identifier,
            target_field=self.target_field,
        ).apply_forecast_input_transformation()

    @property
    @lru_cache()
    def item_metadata(self):
        """
        :return: The item metadata dataset info (from describe_dataset)
        """
        return self._dataset(ITEM_METADATA)

    @property
    @lru_cache()
    def item_metadata_import_job(self):
        """
        :return: The item metadata dataset import job (from list_dataset_import_jobs)
        """
        return self._dataset_import_job(self.item_metadata.DatasetArn)

    @property
    @lru_cache()
    def item_metadata_schema(self) -> Schema:
        """
        :return: The item metadata dataset Schema
        """
        return self._schema(self.item_metadata)

    @property
    @lru_cache()
    def item_metadata_data(self) -> ETL:
        """
        :return: The cleaned item metadata data as an ETL object
        """
        etl = ETL(
            name=ITEM_METADATA,
            schema=self.item_metadata_schema,
            source=self.s3_url(self.item_metadata_import_job),
            identifier=self.identifier,
            target_field=self.target_field,
        )
        transformer = ForecastInputTransformation(etl)
        transformer.apply()
        return etl

    def _schema(self, dataset) -> Union[Schema, None]:
        """
        Get the schema of the dataset specified
        :param dataset: The dataset ("TARGET_TIME_SERIES", "RELATED_TIME_SERIES" or "ITEM_METADATA")
        :return:
        """
        if dataset:
            return Schema(dataset, self.domain)
        return None

    def s3_url(self, resource) -> str:
        """
        Get the S3 URL of the forecast resource
        :param resource: The forecast export job, dataset import job or backtest export job
        :return: S3 URL of the data
        """
        try:
            # output data
            url = resource.Destination["S3Config"]["Path"]
        except AttributeError:
            # input data
            url = resource.DataSource["S3Config"]["Path"]
        return url

    @property
    @lru_cache()
    def forecast(self):
        """
        Get the most recent forecast for this dataset group
        :return: The latest forecast (as a named tuple)
        """
        paginator = self.get_paginator("list_forecasts")(
            Filters=[
                {"Key": "Status", "Value": "ACTIVE", "Condition": "IS"},
                {
                    "Key": "DatasetGroupArn",
                    "Value": self.dataset_group.DatasetGroupArn,
                    "Condition": "IS",
                },
            ]
        )
        matches = []
        for page in paginator:
            matches.extend(page["Forecasts"])

        forecast = next(
            iter((sorted(matches, key=lambda k: k["CreationTime"], reverse=True)))
        )
        return namedtuple("Forecast", forecast)(**forecast)

    @property
    @lru_cache()
    def predictor(self):
        """
        Get the most recent predictor for this dataset group
        :return: The latest predictor (as a named tuple)
        """
        paginator = self.get_paginator("list_predictors")(
            Filters=[
                {"Key": "Status", "Value": "ACTIVE", "Condition": "IS"},
                {
                    "Key": "DatasetGroupArn",
                    "Value": self.dataset_group.DatasetGroupArn,
                    "Condition": "IS",
                },
            ]
        )
        matches = []
        for page in paginator:
            matches.extend(page["Predictors"])

        predictor = self.cli.describe_predictor(
            PredictorArn=next(
                iter((sorted(matches, key=lambda k: k["CreationTime"], reverse=True)))
            )["PredictorArn"]
        )
        return namedtuple("Predictor", predictor)(**predictor)

    @property
    @lru_cache()
    def predictor_backtest_export_job(self):
        """
        Get the most recent predictor backtest export job for this dataset group/ predictor
        :return: The latest predictor backtest export job (as a named tuple)
        """
        paginator = self.get_paginator("list_predictor_backtest_export_jobs")(
            Filters=[
                {"Key": "Status", "Value": "ACTIVE", "Condition": "IS"},
                {
                    "Key": "PredictorArn",
                    "Value": self.predictor.PredictorArn,
                    "Condition": "IS",
                },
            ]
        )
        matches = []
        for page in paginator:
            matches.extend(page["PredictorBacktestExportJobs"])

        predictor_backtest_export_job = next(
            iter((sorted(matches, key=lambda k: k["CreationTime"], reverse=True)))
        )
        return namedtuple("PredictorBacktestExportJob", predictor_backtest_export_job)(
            **predictor_backtest_export_job
        )

    @property
    @lru_cache()
    def predictor_backtest_export_job_data(self):
        """
        :return: The cleaned backtest export job data as an ETL object
        """
        etl = ETL(
            name=PREDICTOR_BACKTEST_EXPORT_JOB,
            schema=self.target_time_series_schema,
            source=f"{self.s3_url(self.predictor_backtest_export_job)}/forecasted-values",
            identifier=self.identifier,
            target_field=self.target_field,
        )
        transformer = ForecastPredictorBacktestExportTransformation(etl)
        transformer.apply()
        return etl

    @property
    @lru_cache()
    def forecast_export_job(self):
        """
        Get the most recent forecast export job for this dataset group/ predictor
        :return: The latest forecast export job (as a named tuple)
        """
        paginator = self.get_paginator("list_forecast_export_jobs")(
            Filters=[
                {"Key": "Status", "Value": "ACTIVE", "Condition": "IS"},
                {
                    "Key": "ForecastArn",
                    "Value": self.forecast.ForecastArn,
                    "Condition": "IS",
                },
            ]
        )
        matches = []
        for page in paginator:
            matches.extend(page["ForecastExportJobs"])

        forecast_export_job = next(
            iter((sorted(matches, key=lambda k: k["CreationTime"], reverse=True)))
        )
        return namedtuple("ForecastExportJob", forecast_export_job)(
            **forecast_export_job
        )

    @property
    @lru_cache()
    def forecast_export_job_data(self):
        """
        :return: The cleaned forecast export job data as an ETL object
        """
        etl = ETL(
            name=FORECAST_EXPORT_JOB,
            schema=self.target_time_series_schema,
            source=self.s3_url(self.forecast_export_job),
            identifier=self.identifier,
            target_field=self.target_field,
        )
        transformer = ForecastExportTransformation(etl)
        transformer.apply()
        return etl

    @lru_cache()
    def _dataset_import_job(self, dataset_arn):
        """
        :param dataset_arn: the dataset ARN of the dataset import job
        :return: the latest dataset improt job (as a named tuple)
        """
        paginator = self.get_paginator("list_dataset_import_jobs")(
            Filters=[
                {"Key": "Status", "Value": "ACTIVE", "Condition": "IS"},
                {"Key": "DatasetArn", "Value": dataset_arn, "Condition": "IS"},
            ]
        )
        matches = []
        for page in paginator:
            matches.extend(page["DatasetImportJobs"])

        dataset_import_job = next(
            iter((sorted(matches, key=lambda k: k["CreationTime"], reverse=True)))
        )
        return namedtuple("DatasetImportJob", dataset_import_job)(**dataset_import_job)

    @lru_cache()
    def _dataset(self, dataset_type):
        """
        :param dataset_type: the dataset type
        :return: the dataset from describe_dataset (as a named tuple)
        """
        dataset_arns = self.dataset_group.DatasetArns
        for dataset_arn in dataset_arns:
            dataset = self.cli.describe_dataset(DatasetArn=dataset_arn)
            dataset = namedtuple("Dataset", dataset)(**dataset)
            if dataset.DatasetType == dataset_type:
                return dataset
        return None

    @property
    @lru_cache()
    def aggregate_forecast_data(self):
        """
        Aggregate all of the forecast data (currently TARGET_TIME_SERIES, PREDICTOR_BACKTEST_EXPORT_JOB,
        FORECAST_EXPORT_JOB, ITEM_METADATA) into a consistent schema for future consumption by Athena
        :return: DynamicFrame representing the consolidated/ aggregated forecast input / output data
        """
        output_schema = ForecastStatus.empty()
        input = self.target_time_series_data
        export = self.forecast_export_job_data
        backtest = self.predictor_backtest_export_job_data

        # apply dimensions to input, export, backtest
        tts_fields = self.target_time_series_schema.fields
        try:
            md_fields = self.item_metadata_schema.fields
        except AttributeError:
            md_fields = []
        attrs = input.map_generic_attribute_names(tts_fields, md_fields)
        attrs = export.map_generic_attribute_names(
            tts_fields, md_fields, attributes=attrs
        )
        attrs = backtest.map_generic_attribute_names(
            tts_fields, md_fields, attributes=attrs
        )

        # drop metadata (will be joined later)
        input.drop_metadata_fields()
        export.drop_metadata_fields()
        backtest.drop_metadata_fields()

        # filter the backtest data out of the input data
        earliest_backtest_data = (
            backtest.df.toDF().select(F.min("timestamp").alias("min")).head()["min"]
        )
        logger.info(
            "taking input TARGET_TIME_SERIES up to %s" % str(earliest_backtest_data)
        )
        filtered_input = input.df.toDF()
        filtered_input = filtered_input.where(
            filtered_input["timestamp"] < earliest_backtest_data
        )

        # combine the data with a union
        aggregate = ForecastStatus.union_dfs(filtered_input, backtest.df.toDF())
        aggregate = ForecastStatus.union_dfs(aggregate, export.df.toDF())

        # add metadata via a join if metadata is available
        try:
            metadata = self.item_metadata_data
            metadata.map_generic_attribute_names(tts_fields, md_fields, attrs)
            metadata_df = metadata.df.toDF()
            aggregate = aggregate.join(metadata_df, ["identifier"], "left")
        except AttributeError:
            logger.info(f"metadata not available to join for {self.name}")

        # prepare the output column format/ order
        aggregate = ForecastStatus.union_dfs(output_schema, aggregate)

        # add the month starting data (this is the partition)
        aggregate = aggregate.withColumn(
            "month_starting",
            F.date_format(F.date_trunc("month", "timestamp"), "y-MM-dd"),
        )

        aggregate_dynamic_frame = DynamicFrame.fromDF(
            aggregate, input.gc, "AGGREGATE_FORECAST"
        )
        return aggregate_dynamic_frame

    @staticmethod
    def empty() -> DataFrame:
        """
        This defines the output schema (without the `month starting` partition column)
        :return: an empty Spark DataFrame with the output schema defined
        """
        sc = SparkContext.getOrCreate()
        gc = GlueContext(sc)

        schema = StructType(
            [
                StructField("timestamp", TimestampType(), True),
                StructField("identifier", StringType(), True),
                StructField("metric", DoubleType(), True),
                *[
                    StructField(f"p{n}", DoubleType(), True) for n in range(1, 100)
                ],  # 99 quantiles 1 through 99
                StructField("mean", DoubleType(), True),
                *[
                    StructField(f"dimension_{n}", StringType(), True)
                    for n in range(1, 11)  # there are 10 dimensions, 1 through 10
                ],
                *[
                    StructField(f"metadata_{n}", StringType(), True)
                    for n in range(1, 11)  # there are 10 metadata fields, 1 through 10
                ],
            ]
        )
        return gc.createDataFrame(sc.emptyRDD(), schema)

    # thanks to https://stackoverflow.com/questions/39758045/how-to-perform-union-on-two-dataframes-with-different-amounts-of-columns-in-spar
    @staticmethod
    def __order_df_and_add_missing_cols(df, columns_order_list, df_missing_fields):
        """ return ordered dataFrame by the columns order list with null in missing columns """
        if not df_missing_fields:  # no missing fields for the df
            return df.select(columns_order_list)
        else:
            columns = []
            for col_name in columns_order_list:
                if col_name not in df_missing_fields:
                    columns.append(col_name)
                else:
                    columns.append(F.lit(None).alias(col_name))
            return df.select(columns)

    @staticmethod
    def __add_missing_columns(df, missing_column_names):
        """ Add missing columns as null in the end of the columns list """
        list_missing_columns = []
        for col in missing_column_names:
            list_missing_columns.append(F.lit(None).alias(col))

        return df.select(df.schema.names + list_missing_columns)

    @staticmethod
    def __order_and_union_d_fs(
        left_df, right_df, left_list_miss_cols, right_list_miss_cols
    ):
        """ return union of data frames with ordered columns by left_df. """
        left_df_all_cols = ForecastStatus.__add_missing_columns(
            left_df, left_list_miss_cols
        )
        right_df_all_cols = ForecastStatus.__order_df_and_add_missing_cols(
            right_df, left_df_all_cols.schema.names, right_list_miss_cols
        )
        return left_df_all_cols.union(right_df_all_cols)

    @staticmethod
    def union_dfs(left_df, right_df):
        """Union between two dataFrames, if there is a gap of column fields,
        it will append all missing columns as nulls"""
        # Check for None input
        if left_df is None:
            raise ValueError("left_df parameter should not be None")
        if right_df is None:
            raise ValueError("right_df parameter should not be None")
            # For data frames with equal columns and order- regular union
        if left_df.schema.names == right_df.schema.names:
            return left_df.union(right_df)
        else:  # Different columns
            # Save dataFrame columns name list as set
            left_df_col_list = set(left_df.schema.names)
            right_df_col_list = set(right_df.schema.names)
            # Diff columns between left_df and right_df
            right_list_miss_cols = list(left_df_col_list - right_df_col_list)
            left_list_miss_cols = list(right_df_col_list - left_df_col_list)
            return ForecastStatus.__order_and_union_d_fs(
                left_df, right_df, left_list_miss_cols, right_list_miss_cols
            )

    def get_paginator(self, name):
        """Utility method to get the paginator for oen of the CLI calls"""
        paginator = self.cli.get_paginator(name)
        return paginator.paginate


class ETL:
    """Used to store and transform Amazon Forecast input, forecast export and predictor backtest export data"""

    def __init__(
        self, name, schema: Schema, source, identifier, target_field, header="detect"
    ):
        """
        :param name: the name of the ETL
        :param schema: the schema of the data used (for exports, use the target time series schema)
        :param source: the data source (s3://data_source/ or s3://data_source/data.csv, fore example)
        :param identifier: the identifier used in this forecast domain
        :param target_field: the target field used in this forecast domain
        :param header: whether or not the data has a header - or if we should auto-detect (e.g. input data)
        """
        self.s3_cli = boto3.client("s3", config=CLIENT_CONFIG)
        self.name = name
        self.schema = schema
        self.source = source
        self.identifier = identifier
        self.target_field = target_field
        if header == "detect":
            self.header = self._detect_header
        else:
            self.header = header

        self.sc = SparkContext.getOrCreate()
        self.gc = GlueContext(self.sc)
        self.sc.getConf().set("spark.sql.session.timeZone", "UTC")

        self.df = self.load()

    def apply_forecast_input_transformation(self):
        """
        Apply forecast input data transformations (used for cleaning forecast input data, mapping fields)
        :return: ETL
        """
        self.df = ForecastInputTransformation(self).apply()
        return self

    def apply_forecast_export_transformation(self):
        """
        Apply forecast export data transformations (mapping fields)
        :return:
        """
        self.df = ForecastExportTransformation(self).apply()
        return self

    def drop_metadata_fields(self):
        """
        Drop metadata fields from this DynamicFrame
        :return:
        """
        metadata_fields = [
            name for name in self.df.toDF().schema.names if METADATA_RE.match(name)
        ]
        self.df = DropFields.apply(
            frame=self.df,
            paths=metadata_fields,
            transformation_ctx="DropMetadataFields",
        )

    def map_generic_attribute_names(
        self, target_time_series_fields, item_metadata_fields, attributes=None
    ):
        """
        Amazon Forecast sometimes returns metadata in exports (in absense of dimensions), and always returns dimensions
        we need to be consistent across naming of these dimensions/ metadata attributes when transforming
        the data.
        :param target_time_series_fields: The list of target time series fields
        :param item_metadata_fields: The list of item metadata fields
        :param attributes: ordered ([dimensions], [metadata])
        :return: attributes: ordered ([dimensions], [metadata])
        """
        names = self.df.toDF().schema.names
        reserved = [
            self.identifier,
            self.target_field,
            "identifier",
            "metric",
            "timestamp",
        ]

        # create or set dimensions and metadata discovered
        if not attributes:
            dimensions = []
            metadata = []
        else:
            dimensions = attributes[0]
            metadata = attributes[1]
        attributes = (dimensions, metadata)

        # populate the list with any newly discovered output fields
        for name in names:
            if all(
                [name not in reserved, name not in dimensions, name not in metadata]
            ):
                if name in target_time_series_fields:
                    dimensions.append(name)
                elif name in item_metadata_fields:
                    metadata.append(name)

        # apply any necessary renaming to the fields in this dataset
        for kind, attrs in [("dimension", dimensions), ("metadata", metadata)]:
            for idx, attribute in enumerate(attrs, start=1):
                attribute_name = f"{kind}_{idx}"
                if attribute in names:
                    logger.info(
                        "%s renaming %s as %s" % (self.name, attribute, attribute_name)
                    )
                    self.df = RenameField.apply(
                        frame=self.df,
                        old_name=attribute,
                        new_name=attribute_name,
                        transformation_ctx=f"Rename{attribute.title()}",
                    )
        return attributes

    def load(self):
        """
        Load the data for an ETL job
        :return: Glue DynamicFrame for this data
        """
        # if a URL was provided, try to load the cached copy first, then revert to S3
        try:
            data_frame = (
                self.gc.read.format("csv")
                .option("header", self.header)
                .option("sep", ",")
                .csv(self.name)
            )
            df = DynamicFrame.fromDF(
                dataframe=data_frame, glue_ctx=self.gc, name=self.name
            )
            logger.info("%s loaded from disk" % self.name)
        except AnalysisException:
            df = self.gc.create_dynamic_frame_from_options(
                connection_type="s3",
                connection_options={
                    "paths": [self.source],
                },
                format="csv",
                format_options={
                    "withHeader": self.header,
                },
            )
            logger.info("%s loaded from s3" % self.name)
        return df

    def _get_export_path(self) -> str:
        """
        Get the first found non-zero output file path involved in a forecast export or predictor backtest export
        :param export_path: the s3 URL of the forecast export or backtest export
        :return: (bucket, key) the bucket and key of the first found non-zero output file
        """
        bucket, key = self._split_s3_url(self.source)

        paginator = self.s3_cli.get_paginator("list_objects_v2")
        iterator = paginator.paginate(Bucket=bucket, Prefix=key)

        # get the first found non-zero CSV file in the export or raise an exception
        for page in iterator:
            for obj in page.get("Contents"):
                if obj["Key"].endswith(".csv") and obj["Size"] != 0:
                    return f"s3://{bucket}/{obj['Key']}"

        raise ValueError(f"Could not find export data at {self.source}")

    @property
    def _detect_header(self):
        """
        Detects a header in a CSV file in S3
        :return: True or False (header present or not present)
        """
        if not self.source.endswith(".csv"):
            source = self._get_export_path()
        else:
            source = self.source

        bucket, key = self._split_s3_url(source)
        obj = self.s3_cli.get_object(Bucket=bucket, Key=key)

        # read up to 100 lines of the .csv file
        # take only non-empty lines as a sample
        sample = ""
        for index, line in zip(range(100), obj["Body"].iter_lines()):
            if line:
                sample += line.decode() + "\n"

        # try manual detection (best for small datasets) - all header fields present
        first_line = sample.splitlines()[0]
        fields = first_line.split(",")
        if sorted(list(fields)) == sorted(list(self.schema.fields)):
            logger.info("%s header present" % (self.source))
            return True

        # try manual detection (best for small datasets) - no header fields present
        if not any(field in list(self.schema.fields) for field in fields):
            logger.info("%s header absent" % (self.source))
            return False

        # try auto detection if manual detection didn't work - this works well for larger files
        try:
            has_header = csv.Sniffer().has_header(sample)
            logger.info(
                "%s header %s" % (self.source, "present" if has_header else "absent")
            )
        except csv.Error:
            # often caused by not being able to determine the delimiter - we can assume there is no header - it will be
            # filtered out by glue transforms and joins, so this is not a concern.
            has_header = False
            logger.warning(
                "%s has input data quality issues please verify your data set quality"
                % self.source
            )

        return has_header

    def _split_s3_url(self, url):
        """
        Split an S3 URL into a tuple (bucket, key)
        :param url: the s3://url/key
        :return: (bucket, key)
        """
        parsed = urlparse(url)
        return parsed.netloc, parsed.path.lstrip("/")


def updates_df_as_dynamicframe(func):
    """
    Decorator to treat updates to `df` as a Glue DynamicFrame
    :param func: the function to wrap
    :return: the wrapped function
    """

    @wraps(func)
    def wrap(self, *args, **kwargs):
        self.df = self.etl.df
        self.etl.df = func(self, *args, **kwargs)

    return wrap


def updates_df_as_dataframe(func):
    """
    Decorator to treat updates to `df` as a Spark DataFrame
    :param func: the function to wrap
    :return: the wrapped function
    """

    @wraps(func)
    def wrap(self, *args, **kwargs):
        self.df = self.etl.df.toDF()
        self.etl.df = DynamicFrame.fromDF(
            func(self, *args, **kwargs), self.etl.gc, f"{self.etl.name}_{func.__name__}"
        )

    return wrap


class ForecastDataTransformation:
    def __init__(self, etl: ETL):
        """
        :param etl: the ETL object containing the data
        """
        self.etl = etl
        self.df: Union[DynamicFrame, DataFrame] = self.etl.df

    @updates_df_as_dataframe
    def drop_duplicates(self):
        """
        Drop all duplicate records
        :return: The DynamicFame
        """
        logger.info("%s dropping duplicates" % self.etl.name)
        return self.df.distinct()


class ForecastPredictorBacktestExportTransformation(ForecastDataTransformation):
    """Transform predictor backtest export data into a format that can be consolidated with input data"""

    def __init__(self, etl: ETL):
        """
        :param etl: the ETL object containing the data
        """
        super().__init__(etl)
        self.output_dimensions = []

    def apply(self):
        """Apply all transformations"""
        self.drop_duplicates()
        self.apply_input_mappings()

    @updates_df_as_dynamicframe
    def apply_input_mappings(self):
        """
        Map all fields to those supported in consolidating
        :return: The DynamicFame
        """
        logger.info("%s applying schema mappings" % self.etl.name)

        fields = list(self.df.schema().field_map.keys())
        mappings = list()
        for field in fields:
            if field == self.etl.identifier:
                mappings.append((field, "string", "identifier", "string"))
            elif field == "timestamp" or field == "date":
                mappings.append((field, "string", "timestamp", "timestamp"))
            elif field == self.etl.target_field or field == "target_value":
                mappings.append((field, "string", "metric", "double"))
            elif QUANTILE_RE.match(field):
                mappings.append((field, "string", field, "double"))
            elif (
                field == "backtestwindow_start_time"
                or field == "backtestwindow_end_time"
            ):
                pass  # ignore - forecast does not allow overlapping backtest windows as of 2021-02-04
            else:
                self.output_dimensions.append(field)
                mappings.append((field, "string", field, "string"))

        return ApplyMapping.apply(
            frame=self.df,
            mappings=mappings,
            transformation_ctx="ApplyMapping",
        )


class ForecastExportTransformation(ForecastDataTransformation):
    """Transform forecast export data into a format that can be consolidated with input data"""

    def __init__(self, etl: ETL):
        super().__init__(etl)
        self.output_dimensions = []

    def apply(self):
        """Apply all transformations"""
        self.drop_duplicates()
        self.apply_input_mappings()

    @updates_df_as_dynamicframe
    def apply_input_mappings(self):
        """
        Map all fields to those supported in consolidating
        :return: The DynamicFame
        """
        logger.info("%s applying schema mappings" % self.etl.name)

        fields = list(self.df.schema().field_map.keys())
        mappings = list()
        for field in fields:
            if field == self.etl.identifier:
                mappings.append((field, "string", "identifier", "string"))
            elif field == "timestamp" or field == "date":
                mappings.append((field, "string", "timestamp", "timestamp"))
            elif QUANTILE_RE.match(field):
                mappings.append((field, "string", field, "double"))
            else:
                self.output_dimensions.append(field)
                mappings.append((field, "string", field, "string"))

        return ApplyMapping.apply(
            frame=self.df,
            mappings=mappings,
            transformation_ctx="ApplyMapping",
        )


class ForecastInputTransformation(ForecastDataTransformation):
    """Transform forecast input data into a format that can be consolidated with input data"""

    def __init__(self, etl: ETL):
        super().__init__(etl)

    def apply(self):
        """Apply all transformations"""
        self.drop_null_fields()
        self.filter_timestamps()
        self.apply_input_mappings()
        self.filter()
        self.select_fields()
        self.rename_target_field()
        self.rename_identifier()

    @updates_df_as_dataframe
    def apply_bucketing(self, dimensions: List[str], frequency: str):
        """
        Apply binning/ bucketing to the dataframe/ dynamicframe (forecast data can be unordered and sparse)
        :param dimensions: the predictor dimensions
        :param frequency: the predictor frequency
        :return:
        """
        window = {
            "Y": F.trunc("timestamp", "year"),
            "M": F.trunc("timestamp", "month"),
            "W": F.window("timestamp", "1 week", startTime="4 day"),
            "D": F.window("timestamp", "1 day"),
            "H": F.window("timestamp", "1 hour"),
            "30min": F.window("timestamp", "30 minutes"),
            "15min": F.window("timestamp", "15 minutes"),
            "10min": F.window("timestamp", "10 minutes"),
            "5min": F.window("timestamp", "5 minutes"),
            "1min": F.window("timestamp", "1 minute"),
        }[frequency]

        df = self.df
        if frequency in ["Y", "M"]:
            # group over the truncated date
            df = (
                df.groupBy("identifier", *dimensions, window.alias("timestamp"))
                .agg(F.sum("metric").alias("metric"))
                .orderBy("identifier", *dimensions, "timestamp", ascending=True)
            )
        else:
            # group over the window
            group = df.groupBy("identifier", *dimensions, window).agg(
                F.sum("metric").alias("metric")
            )
            df = group.select(
                "identifier",
                *dimensions,
                group.window.start.alias("timestamp"),
                "metric",
            ).orderBy("identifier", *dimensions, "timestamp", ascending=True)
        return df

    @updates_df_as_dynamicframe
    def drop_null_fields(self):
        """
        :return: DynamicFrame with empty rows dropped (data cleaning)
        """
        logger.info("%s dropping empty rows" % self.etl.name)
        return DropNullFields.apply(
            frame=self.df,
            transformation_ctx="DropNullFields",
        )

    @updates_df_as_dynamicframe
    def apply_input_mappings(self):
        """
        :return: DynamicFrame mapped to the schema data types
        """
        logger.info("%s applying schema mappings" % self.etl.name)
        return ApplyMapping.apply(
            frame=self.df,
            mappings=self.etl.schema.mappings(self.etl.df),
            transformation_ctx="ApplyMapping",
        )

    @updates_df_as_dataframe
    def filter_timestamps(self):
        """
        :return: DynamicFrame with all invalid timestamps filtered out
        """
        if "timestamp" not in self.etl.schema.fields:
            logger.info("%s has no timestamp field" % self.etl.name)
            return self.df

        logger.info("%s removing data with invalid timestamps" % self.etl.name)
        timestamp_index = self.etl.schema.fields.index("timestamp")
        timestamp_field = self.etl.df.toDF().schema.fieldNames()[timestamp_index]
        return self.df.filter(self.df[timestamp_field].rlike(TIME_RE))

    @updates_df_as_dynamicframe
    def filter(self):
        """
        :return: DynamicFrame with null values in non-target fields filtered out (data cleaning)
        """
        logger.info("%s removing rows with empty non-target fields" % self.etl.name)
        non_target_fields = [
            field for field in self.etl.schema.fields if field != self.etl.target_field
        ]
        return Filter.apply(
            frame=self.df,
            f=lambda x: all(x[field] is not None for field in non_target_fields),
            transformation_ctx="Filter",
        )

    @updates_df_as_dynamicframe
    def select_fields(self):
        """
        :return: DynamicFrame with column headers added
        """
        logger.info("%s adding column headers" % self.etl.name)
        fields = self.etl.schema.fields
        return SelectFields.apply(
            frame=self.df,
            paths=fields,
            transformation_ctx="SelectFields",
        )

    @updates_df_as_dataframe
    def rename_target_field(self):
        """
        :return: DynamicFrame with target field renamed
        """
        old_name = self.etl.target_field
        logger.info("%s renaming target field %s to metric" % (self.etl.name, old_name))
        return self.df.withColumnRenamed(old_name, "metric")

    @updates_df_as_dataframe
    def rename_identifier(self):
        """
        :return: DynamicFrame with identifier renamed
        """
        old_name = self.etl.identifier
        logger.info(
            "%s renaming identifier field %s to identifier" % (self.etl.name, old_name)
        )
        return self.df.withColumnRenamed(old_name, "identifier")


if __name__ == "__main__":
    """This is the main entrypoint for AWS Glue"""

    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "region",
            "account_id",
            "dataset_group",
            "database",
            "data_bucket",
            "glue_table_name",
        ],
    )

    sc = SparkContext.getOrCreate()
    gc = GlueContext(sc, catalog_id=args["account_id"])

    forecast = ForecastStatus(
        args["dataset_group"], region=args["region"], account=args["account_id"]
    )
    aggregated_data = forecast.aggregate_forecast_data

    # sync to s3
    logger.info("Syncing data to S3")
    sink = gc.getSink(
        connection_type="s3",
        path=f"s3://{args['data_bucket']}/output/{args['glue_table_name']}",
        enableUpdateCatalog=True,
        updateBehavior="UPDATE_IN_DATABASE",
        partitionKeys=["month_starting"],
    )
    sink.setFormat("glueparquet")
    sink.setCatalogInfo(
        catalogDatabase=args["database"], catalogTableName=args["glue_table_name"]
    )
    sink.writeFrame(aggregated_data)
