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

import codecs
import csv
from collections import OrderedDict
from concurrent import futures
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Union, Any, List
from urllib.parse import urlparse
from uuid import uuid4 as uuid

from dateutil.relativedelta import relativedelta
from jinja2 import Environment, FileSystemLoader
from pyathena import connect
from pyathena.async_cursor import AsyncCursor

from shared.Dataset.dataset import Dataset
from shared.Dataset.dataset_file import DatasetFile
from shared.Dataset.dataset_type import DatasetType
from shared.DatasetGroup.dataset_group import Schema
from shared.ETL.models.forecast_export import ForecastExportModel
from shared.ETL.models.item_metadata import MetadataModel
from shared.ETL.models.query import Query, MaximumDate
from shared.ETL.models.target_time_series import TargetTimeSeriesModel
from shared.Forecast.forecast import Forecast
from shared.config import Config
from shared.helpers import get_aws_region, get_s3_client
from shared.logging import get_logger

logger = get_logger(__name__)

PARTITION_DATA_ABOVE = (
    "D"  # adjust this to Y|M|D|W|D if partitions are required for lower frequency data.
)
MAX_METADATA_FIELDS = 10
MAX_DIMENSIONS = 10

loader = FileSystemLoader(
    searchpath=Path(__file__).parent.absolute().joinpath("templates")
)
j2env = Environment(loader=loader)


class InvalidForecastExport(Exception):
    pass


@dataclass
class Column:
    term: str = field(default="")
    alias: str = field(default="")
    expression: str = field(default="")
    schema: Schema = field(default=None)


def prepare_output_columns(
    n_dimensions=MAX_DIMENSIONS, n_metadata=MAX_METADATA_FIELDS
) -> OrderedDict:
    def dimension(n):
        return f"dimension_{n}"

    def metadata(n):
        return f"metadata_{n}"

    columns = OrderedDict(
        [
            (
                "timestamp",
                Column(
                    alias="timestamp",
                    expression='cast(from_iso8601_timestamp(coalesce(ts_data.isotime, ex_data.isotime)) as timestamp) as "timestamp"',
                ),
            ),
            ("identifier", Column()),
            ("metric", Column()),
            *[
                (
                    f"p{n}",
                    Column(alias=f"p{n}", expression=f"cast(NULL as double) AS p{n}"),
                )
                for n in range(1, 100)
            ],
            *[
                (
                    dimension(n),
                    Column(
                        alias=dimension(n),
                        expression=f"cast(NULL as varchar) AS {dimension(n)}",
                    ),
                )
                for n in range(1, n_dimensions + 1)
            ],
            *[
                (
                    metadata(n),
                    Column(
                        alias=metadata(n),
                        expression=f"cast(NULL as varchar) AS {metadata(n)}",
                    ),
                )
                for n in range(1, n_metadata + 1)
            ],
            (
                "month_starting",
                Column(
                    alias="month_starting",
                    expression="date_format(cast(from_iso8601_timestamp(coalesce(ts_data.isotime, ex_data.isotime)) as timestamp), '%Y-%m-01') as month_starting",
                ),
            ),
        ]
    )
    return columns


@dataclass
class DatasetFileDataset:
    dataset: Dataset
    dataset_file: DatasetFile


@dataclass
class ForecastETL:
    """Generate Athena tables from Amazon Forecast input/ exports."""

    workgroup: str
    schema: str
    config: Config
    dataset_file: DatasetFile
    forecast: Forecast
    unique_id: str = field(init=False, default_factory=lambda: f"t_{uuid().hex}")
    target_time_series: DatasetFileDataset = field(init=False)
    item_metadata: DatasetFileDataset = field(init=False)
    related_time_series: DatasetFileDataset = field(init=False)
    s3_cli: Any = field(default_factory=get_s3_client)

    def __post_init__(self):
        # set up the datasets
        (
            self.target_time_series,
            self.related_time_series,
            self.item_metadata,
        ) = self._get_datasets()

        # get the dataset group (from the provided forecast)
        self.dataset_group = self.forecast._dataset_group

        # set up column mappings
        column_mappings = {}
        for n in range(1, MAX_DIMENSIONS + 1):
            column_mappings[f"dimension_{n}"] = None
        for n in range(1, MAX_METADATA_FIELDS + 1):
            column_mappings[f"metadata_{n}"] = None
        self.column_mappings = column_mappings

        # set up the connection to AWS Athena
        self.cursor = connect(
            region_name=get_aws_region(),
            work_group=self.workgroup,
            schema_name=self.schema,
            cursor_class=AsyncCursor,
        ).cursor()

    def _get_datasets(
        self,
    ) -> (
        Union[None, DatasetFileDataset],
        Union[None, DatasetFileDataset],
        Union[None, DatasetFileDataset],
    ):
        """
        Gets the datasets and dataset files associated with this forecast
        :return: (ts, rts, md)
        """
        datasets = self.config.datasets(self.dataset_file)
        prefix = f"s3://{self.dataset_file.bucket}/train/{self.dataset_file.prefix}"

        ts, rts, md = None, None, None

        for dataset in datasets:
            if dataset.dataset_type == DatasetType.TARGET_TIME_SERIES:
                ts = DatasetFileDataset(
                    dataset,
                    DatasetFile.from_s3_path(prefix + dataset.dataset_type.suffix),
                )
            elif dataset.dataset_type == DatasetType.RELATED_TIME_SERIES:
                rts = DatasetFileDataset(
                    dataset,
                    DatasetFile.from_s3_path(prefix + dataset.dataset_type.suffix),
                )
            elif dataset.dataset_type == DatasetType.ITEM_METADATA:
                md = DatasetFileDataset(
                    dataset,
                    DatasetFile.from_s3_path(prefix + dataset.dataset_type.suffix),
                )

        return (ts, rts, md)

    def _copy_dataset(self, source: DatasetFileDataset) -> DatasetFileDataset:
        """
        Athena works against folders of .csv files, but not single .csv files. This copies them to a temporary location
        under the forecast data bucket (under /raw) to consume their data properly
        :param source: DatasetFileDataset of source input
        :return: DatasetFileDataset of destination (under 'raw')
        """
        dest = source.dataset_file.copy(
            "raw", self.unique_id, str(source.dataset_file.data_type)
        )
        copied_dataset_file = DatasetFile(key=dest, bucket=source.dataset_file.bucket)
        return DatasetFileDataset(
            dataset=source.dataset, dataset_file=copied_dataset_file
        )

    @property
    def datasets(self) -> List[DatasetFileDataset]:
        """
        Get all of the datasets that are present for this consolidation job
        :return: List[DatasetFileDataset]
        """
        return [
            i
            for i in [
                self.target_time_series,
                self.related_time_series,
                self.item_metadata,
            ]
            if i
        ]

    def input_table_names(self):
        """
        Get the input table name for each dataset present for this consolidation job
        :return:
        """
        return [self.input_table_name(i) for i in self.datasets]

    def input_table_name(
        self, source: Union[None, DatasetFileDataset]
    ) -> Union[str, None]:
        """
        Get the input table name for the DatasetFileDataset
        :param source:
        :return: str
        """
        if not source:
            return None

        ext = ""
        if source.dataset.dataset_type == DatasetType.ITEM_METADATA:
            ext = "_metadata"
        elif source.dataset.dataset_type == DatasetType.RELATED_TIME_SERIES:
            ext = "_related"

        table_name = f"{self.unique_id}"
        table_name = table_name + ext if ext else table_name

        return table_name

    def export_table_name(self) -> str:
        """Get the export table name"""
        return f"{self.unique_id}_export"

    def _validate_futures(self, fs, timeout=60, abandon_if_failed=True):
        """
        Validate the concurrent futures (fs) passed in, failing if timeout elapses or a future has an exception
        :param fs: list of concurrent futures
        :param timeout: the timeout
        :return: future results
        """
        futures.wait(fs, timeout=timeout)
        excs = []
        for f in fs:
            exc = f.exception()
            if exc:
                excs.append(str(exc))
        if excs:
            raise ValueError(f"Athena Error: {', '.join(excs)}")

        results = [f.result() for f in fs]
        for result in results:
            if result.state == "FAILED":
                error = "failed athena %s request %s: %s" % (
                    result.statement_type,
                    result.query_id,
                    result.state_change_reason,
                )
                logger.error(error)
                if abandon_if_failed:
                    raise ValueError(error)

        return [f.result() for f in fs]

    def cleanup_temp_tables(self):
        """
        Clean up temporary tables used in generating the consolidated forecast table in Athena
        :return: List[concurrent futures]
        """

        def cleanup(table_name):
            logger.info(
                f"removing table {table_name} in workgroup {self.workgroup} if it exists"
            )
            _, future = self.cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
            return future

        fs = [cleanup(table_name) for table_name in self.input_table_names()]
        fs.append(cleanup(self.export_table_name()))
        return self._validate_futures(fs)

    def create_input_tables(self):
        """
        Create all temporary tables for the forecast input. These are used once and then destroyed.
        :return: The result from all futures
        """
        domain_schema = self.dataset_group.schema
        fs = []

        # temporary table for forecast export
        export = self._get_forecast_export()
        bucket, key = self._get_export_path(export)
        export_columns = self._get_export_columns(bucket, key)
        model = ForecastExportModel(domain_schema, export_columns)
        fs.append(
            self._create_table(
                table_name=self.export_table_name(),
                properties=model.properties,
                s3_path=self._get_forecast_export()["Destination"]["S3Config"]["Path"],
                skip_header_line_count=1,
            )
        )

        # temporary table for forecast target time series
        if self.target_time_series:
            self.target_time_series = self._copy_dataset(self.target_time_series)
            model = TargetTimeSeriesModel(
                domain_schema,
                self.target_time_series.dataset.dataset_schema["Attributes"],
            )
            fs.append(
                self._create_table(
                    table_name=self.input_table_name(self.target_time_series),
                    properties=model.properties,
                    s3_path=self.target_time_series.dataset_file.s3_prefix,
                )
            )

        # temporary table for forecast item metadata
        if self.item_metadata:
            self.item_metadata = self._copy_dataset(self.item_metadata)
            model = MetadataModel(
                domain_schema, self.item_metadata.dataset.dataset_schema["Attributes"]
            )
            fs.append(
                self._create_table(
                    table_name=self.input_table_name(self.item_metadata),
                    properties=model.properties,
                    s3_path=self.item_metadata.dataset_file.s3_prefix,
                )
            )

        # note: related data has not yet been added to the consolidated export at this stage

        return self._validate_futures(fs, timeout=300)

    def _earliest_date(self):
        """
        We currently limit data to the past 8 years from the end of the forecast horizon (this limits us to 96
        partitions for data consolidation.
        :return: str
        """
        export_table_name = self.export_table_name()

        query = Query(
            cursor=self.cursor,
            query=f"SELECT concat(substring(max(date), 1, 7), '-01') as max_date from {export_table_name}",
            model=MaximumDate,
        )

        max_date: MaximumDate = next(query.results).as_date
        earliest_date = max_date - relativedelta(years=8)

        return earliest_date

    @property
    def output_table_name(self):
        export = self._get_forecast_export()
        return export.get("ForecastExportJobName")

    def consolidate_data(self):
        """
        Generate the consolidated forecast export in Athena from the timeseries data, metadata and forecast export
        :return: The result from all futures
        """
        domain_schema = self.dataset_group.schema
        timestamp_format = self.config.data_timestamp_format(
            self.target_time_series.dataset_file
        )

        # get the export model
        export = self._get_forecast_export()
        bucket, key = self._get_export_path(export)
        export_columns = self._get_export_columns(bucket, key)
        ex_model = ForecastExportModel(domain_schema, export_columns)
        ex_model.set_timestamp_format(timestamp_format)

        # get the target time series model
        create_partitions = False
        if self.target_time_series:
            self.target_time_series = self._copy_dataset(self.target_time_series)
            ts_model = TargetTimeSeriesModel(
                domain_schema,
                self.target_time_series.dataset.dataset_schema["Attributes"],
            )
            ts_model.set_timestamp_format(timestamp_format)
            create_partitions = (
                self.target_time_series.dataset.data_frequency > PARTITION_DATA_ABOVE
            )

        # get the metadata model
        md_model = None
        if self.item_metadata:
            self.item_metadata = self._copy_dataset(self.item_metadata)
            md_model = MetadataModel(
                domain_schema, self.item_metadata.dataset.dataset_schema["Attributes"]
            )

        output_table_name = export.get("ForecastExportJobName")
        output = prepare_output_columns(MAX_DIMENSIONS, MAX_METADATA_FIELDS)
        for k, v in output.items():
            if k in ts_model and k in ex_model:
                v.expression = f"coalesce(ts_data.{k}, ex_data.{k}) as {k}"
            elif k in ts_model:
                v.expression = f"ts_data.{k} as {k}"
            elif k in ex_model:
                v.expression = f"ex_data.{k} as {k}"
            elif md_model and k in md_model:
                v.expression = f"md_data.{k} as {k}"

        template = j2env.get_template(f"consolidate_query.template")
        ctas = template.render(
            output_table_name=output_table_name,
            ts_table=self.input_table_name(self.target_time_series),
            ts_expressions=", ".join(
                [
                    column.expression
                    if column.expression
                    else f"{column.term} as {column.alias}"
                    for column in ts_model.columns
                ]
            ),
            ex_table=self.export_table_name(),
            ex_expressions=", ".join(
                [
                    column.expression
                    if column.expression
                    else f"{column.term} as {column.alias}"
                    for column in ex_model.columns
                ]
            ),
            md_table=self.input_table_name(self.item_metadata),
            md_expressions=", ".join(
                [
                    column.expression
                    if column.expression
                    else f"{column.term} as {column.alias}"
                    for column in md_model.columns
                ]
            )
            if md_model
            else None,
            output_map=", ".join([v.expression for k, v in output.items()]),
            create_partitions=create_partitions,
            earliest_date=self._earliest_date(),
        )

        _, future = self.cursor.execute(ctas)
        return self._validate_futures([future], timeout=300)

    def _get_forecast_export(self):
        exports = self.forecast.export_history()
        if len(exports) < 1:
            raise ValueError(f"could not find exports for {self.dataset_file.name}")
        return exports[0]

    def _get_export_path(self, export: Dict) -> (str, str):
        """
        Get the first found non-zero output file path involved in a forecast export
        :param export: the forecast export job as returned by list_forecast_export_jobs
        :return: (bucket, key) the bucket and key of the first found non-zero output file
        """
        export_path = export.get("Destination", {}).get("S3Config", {}).get("Path")
        if not export_path:
            raise ValueError("Could not find export destination s3 configuration path")

        parsed = urlparse(export_path, allow_fragments=False)
        bucket = parsed.netloc
        key = parsed.path.lstrip("/")

        paginator = self.s3_cli.get_paginator("list_objects_v2")
        iterator = paginator.paginate(Bucket=bucket, Prefix=key)

        # get the first found non-zero CSV file in the export or raise an exception
        for page in iterator:
            for obj in page.get("Contents"):
                if obj["Key"].endswith(".csv") and obj["Size"] != 0:
                    return bucket, obj["Key"]

        raise ValueError(f"Could not find forecast output at {export_path}")

    def _get_export_columns(self, bucket, key):
        """
        Get the column names from the forecast export job result .csv
        :param bucket: the bucket containing the export job .csv
        :param key: the key of a non-zero-byte forecast export job .csv file
        :return:
        """
        response = self.s3_cli.get_object(Bucket=bucket, Key=key)
        stream_reader = codecs.getreader("utf-8")(response["Body"])
        reader = csv.reader(stream_reader)
        return next(reader)

    def _create_table(
        self,
        table_name,
        properties,
        s3_path,
        delimiter=",",
        skip_header_line_count: int = 0,
    ):
        """
        Create a temporary input data table (works for time series, related time series, metadata, forecast export job)
        :param table_name: the name of the temporary table
        :param properties: the properties of the temporary table (as a comma separated list of column_name column_type)
        :param s3_path: the s3 path for the athena create table statement
        :param delimiter: the delimiter to use
        :param skip_header_line_count: set to 1 to skip the header (used in forecast export jobs)
        :return:
        """
        skip_header_line_count = str(skip_header_line_count)

        if not s3_path.endswith("/"):
            s3_path += "/"

        logger.info(f"table {table_name} is creating")
        query = """
            CREATE EXTERNAL TABLE IF NOT EXISTS {table} ({properties})
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
            WITH SERDEPROPERTIES (
                'serialization.format' = ',',
                'field.delim' = '{delimiter}')
            LOCATION '{input_path}'
            TBLPROPERTIES ('has_encrypted_data'='false', 'skip.header.line.count'='{skip_header_line_count}');
            """.format(
            table=table_name,
            properties=properties,
            delimiter=delimiter,
            input_path=s3_path,
            skip_header_line_count=skip_header_line_count,
        )
        _, future = self.cursor.execute(query)
        return future
