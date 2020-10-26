#!/usr/bin/env python
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

from __future__ import annotations

import csv
import logging
import re
from collections import OrderedDict
from datetime import datetime
from typing import List

import click
import matplotlib.pyplot as plt
import numpy as np
import yaml
from dateutil.relativedelta import relativedelta
from matplotlib.dates import AutoDateFormatter, AutoDateLocator, date2num

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler()
formatter = logging.Formatter(
    "%(asctime)s - create_synthetic_data - %(levelname)s - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)

valid_date = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$|^\d{4}-\d{2}-\d{2}$")


OUTPUT_TIMESERIES_FILE = "ts.csv"
OUTPUT_METADATA_FILE = "ts.metadata.csv"


def validate_date(ctx, param, value):
    """
    Validate the date passed in from the command line
    :param ctx: the click context (required, not used)
    :param param: the parameter (required, not used)
    :param value: the value of the parameter
    :return: datetime
    """
    if valid_date.match(value):
        return datetime.fromisoformat(value)
    else:
        raise click.BadParameter(
            "bad start date - use formats matching yyyy-MM-dd or yyyy-MM-dd HH:mm:ss"
        )


def validate_frequency(ctx, param, value):
    """
    Validate a frequency is compatible with Amazon Forecast supported frequencies (Y|M|W|D|30min|15min|10min|5min|1min)
    :param ctx: the click context (required, not used)
    :param param: the parameter (required, not used)
    :param value: the value of the parameter (as a string matching the above supported frequencies)
    :return: the frequency, as a Python relativedelta
    """
    if value == "Y":
        value = relativedelta(years=+1)
    elif value == "M":
        value = relativedelta(months=+1)
    elif value == "W":
        value = relativedelta(weeks=+1)
    elif value == "D":
        value = relativedelta(days=+1)
    elif value == "H":
        value = relativedelta(hours=+1)
    elif value == "30min":
        value = relativedelta(minutes=+30)
    elif value == "15min":
        value = relativedelta(minutes=+15)
    elif value == "10min":
        value = relativedelta(minutes=+10)
    elif value == "5min":
        value = relativedelta(minutes=+5)
    elif value == "1min":
        value = relativedelta(minutes=+1)
    else:
        raise click.BadParameter(
            "invalid frequency - must be one of Y|M|W|D|H|30min|15min|10min|5min|1min"
        )

    return value


class Dependency:
    """
    Represents a dependency between two models
    """

    def __init__(self, model, chance, influences_zero_rate=False):
        """
        Create a dependency object
        :param model: the model to depend on
        :param chance: the chance that demand in that model influences our own demand
        :param influences_zero_rate: whether or not if our rate is zero, we should be influenced by the dependent model
        """
        self.model = model
        self.chance = chance
        self.influences_zero_rate = influences_zero_rate


class Metadata:
    """
    Represents a forecast metadata attribute and its impacts to an item's demand
    """

    def __init__(self, name: str, value: str, min: float = 1.0, max: float = 1.0):
        """
        Create a new metadata object
        :param name: the name of the metadata attribute
        :param value: the value of the metadata attribute
        :param min: an estimated minimum of the amount this metadata impacts item demand
        :param max: an estimated maximum of the amount this metadata impacts item demand
        """

        self.name = name
        self.value = value
        self.min = min
        self.max = max

    def __repr__(self):
        return f'Metadata(name="{self.name}", value="{self.value}", min={self.min}, max={self.max})'

    def __eq__(self, other):
        if not isinstance(other, Metadata):
            return False
        return (
            self.name == other.name
            and self.value == other.value
            and self.min == other.min
            and self.max == other.max
        )

    @property
    def rate(self):
        """
        Get a random rate that conforms to a normal distribution centred about the midpoint of min/max
        :return: a sample of the rate. 99.7% of rates should fall between min and max and be normally distributed about
        the midpoint between them
        """
        if self.min > self.max or self.max < self.min:
            raise ValueError("ensure minimum is less than or equal to maximum")
        if self.min <= 0 or self.max <= 0:
            raise ValueError("ensure minimum and maximum are above zero")

        loc = (self.min + self.max) / 2.0
        scale = (
            self.max - loc
        ) / 3.0  # 99.7% of data should fall between min/ max centred about loc
        return np.random.normal(loc=loc, scale=scale)


class MetadataExport:
    """
    Represents a metadata export operation
    """

    def __init__(self):
        self._metadata = {}

    def add(self, item_name: str, metadata: Metadata):
        """
        Add item metadata to the export
        :param item_name: the name of the item
        :param metadata: the metadata attribute to add
        :return:
        """
        item_metadata = self._metadata.get(item_name, {})
        if not item_metadata:
            self._metadata[item_name] = {}
            item_metadata = self._metadata[item_name]

        set_metadata = item_metadata.get(metadata.name)
        if set_metadata and set_metadata != metadata.value:
            raise ValueError(f"conflicting item metadata for item {item_name}")
        if not set_metadata:
            item_metadata[metadata.name] = metadata.value

    def export(self):
        """
        Perform the metadata export (exports to OUTPUT_METADATA_FILE)
        :return: None
        """
        template_item = list(self._metadata.keys())
        if len(template_item) == 0:
            return

        # do some validation
        template_item = next(iter(template_item))
        template_keys = list(self._metadata.get(template_item).keys())
        for k, _ in self._metadata.items():
            if sorted(template_keys) != sorted(list(self._metadata.get(k).keys())):
                raise ValueError(
                    "all items must share the same metadata attributes (but can have different values)"
                )

        md_file = open(OUTPUT_METADATA_FILE, "a+", newline="")
        try:
            md_writer = csv.writer(
                md_file, quoting=csv.QUOTE_MINIMAL, dialect=csv.unix_dialect
            )
            for k, v in self._metadata.items():
                row = [k]

                # order the metadata by name and write it
                ordered = OrderedDict(sorted(v.items(), key=lambda t: t[0]))
                row.extend([v for _, v in ordered.items()])
                md_writer.writerow(row)
        finally:
            md_file.close()


class TimeSeriesModel:
    """
    Represent a time series (e.g. retail demand data)
    """

    def __init__(
        self,
        name: str,
        start: datetime,
        rate: float,
        per: relativedelta,
        output: relativedelta,
    ):
        """
        Initialize a new time series model
        :param name: The name of the model
        :param start: The start date
        :param rate: The expected rate of occurrences (e.g. demand per second)
        :param per: The frequency to use for the rate
        :param output: The frequency to use for our output (must be the same across all models)
        """
        self.name = name
        self.start = start
        self.delta = output
        self.dependencies: List[Dependency] = []
        self.metadata: List[Metadata] = []
        self.dimensions = {}

        # calculate the bucket size
        _bucket_end_d = start + output
        self._bucket_size_m = (_bucket_end_d - start).total_seconds() / 60.0

        # rate provided at frequency specified (e.g. frequency D, 100 for 100/d)
        self._frequency_m = ((start + per) - start).total_seconds() / 60.0
        self._rate_m = rate / self._frequency_m

        # what is the rate required for the output window?
        self._rate = self._rate_m * self._bucket_size_m
        if self._rate > 15.0:
            logger.warning(
                "large rates increase execution time - this might take some time"
            )

        # rate multipliers period over period
        self._rate_multiplier = 1

        self.hourly_seasonality = [1 for _ in range(24)]
        self.daily_seasonality = [1 for _ in range(7)]
        self.monthly_seasonality = [1 for _ in range(12)]

        self._data = []
        self._data_dependencies = []
        self._arrival_time = 0.0
        self._last_updated = -1

    def add_metadata(self, name: str, value: str, min: float = 0.9, max: float = 1.1):
        """
        Add metadata to this model
        :param name: the name of the metadata attribute to add
        :param value: the value of the metadata attribute to add
        :param min: the minimum rate adjustment expected for items of this sort
        :param max: the maximum rate adjustment expected for items of this sort
        :return: None
        """
        metadata = Metadata(name, value, min=min, max=max)
        self.metadata.append(metadata)

    def add_dependency(
        self, model: TimeSeriesModel, chance: float, influences_zero_rate: bool = False
    ):
        """
        Add a model dependency to this model
        :param model: the model that this model depends on
        :param chance: the chance that one occurrence in the same time interval of the dependent model results in a
        change to our model. this can be positive (complementary goods) or negative (substitute goods)
        :param influences_zero_rate: whether or not the dependent model impacts a zero rate of this model
        :return: None
        """
        self.dependencies.append(
            Dependency(model, chance, influences_zero_rate=influences_zero_rate)
        )

    def set_dimension(self, name: str, value: str):
        """
        Set a model dimension (for example, location, segment, geography)
        :param name: the name of the attribute
        :param value: the value of the attribute
        :return: None
        """
        self.dimensions[name] = value

    @property
    def rate(self):
        """
        Get the current rate
        :return: the current rate
        """
        return self._rate

    @property
    def mean(self):
        """
        Get the mean of the currently generated data (useful for validation)
        :return: the mean of the currently generated data
        """
        return sum(self._data[:-1]) / (len(self._data) - 1)

    def set_hourly_seasonalities(self, adjustments=List[float]):
        """
        Set hourly seasonalities
        :param adjustments: hourly rate adjustment array, starting at 00:00
        :return: None
        """
        if len(adjustments) != 24:
            raise ValueError(
                "You must specify a rate for each of 24 hours (starting at 00:00)"
            )
        self.hourly_seasonality = adjustments

    def set_daily_seasonalities(self, adjustments=List[float]):
        """
        Set daily seasonalities
        :param adjustments: daily rate adjustment array, starting Monday
        :return: None
        """
        if len(adjustments) != 7:
            raise ValueError(
                "You must specify a rate for each of 7 days (starting Monday)"
            )
        self.daily_seasonality = adjustments

    def set_monthly_seasonalities(self, adjustments=List[float]):
        """
        Set monthly seasonalities
        :param adjustments: monthly rate adjustment array, starting January
        :return: None
        """
        if len(adjustments) != 12:
            raise ValueError(
                "You must specify a rate for each of 12 months (starting January)"
            )
        self.monthly_seasonality = adjustments

    def rate_at(self, time):
        """
        Get the rate, adjusted by seasonalities and metadata at the time specified
        :param time:
        :return: the rate
        """
        delta = relativedelta(minutes=+(self._bucket_size_m * time))

        # handle seasonality
        time = self.start + delta
        rate = (
            self.rate
            * self.hourly_seasonality[time.hour]
            * self.daily_seasonality[time.weekday()]
            * self.monthly_seasonality[time.month - 1]
        )

        # handle metadata adjustments
        for metadata in self.metadata:
            rate *= metadata.rate

        return rate

    def calculate_dependencies(self):
        """
        A naive method of introducing dependencies that allows for interrelated dependencies.
        :return: None
        """
        self._data_dependencies = self._data.copy()
        for idx, count in enumerate(self._data_dependencies[: self._last_updated]):
            for dependency in self.dependencies:
                try:
                    for n in range(dependency.model._data[idx]):
                        p = np.random.random()

                        if dependency.chance > 0 and p < dependency.chance:
                            self._data_dependencies[idx] += 1
                        elif dependency.chance < 0 and p > dependency.chance:
                            self._data_dependencies[idx] -= 1
                except IndexError:
                    pass

            self._data_dependencies[idx] = max(self._data_dependencies[idx], 0)

    def finalize_dependencies(self):
        """Finalize the dependencies by overwriting the data array"""
        logger.info(f"finalized dependencies for {self.name}")
        self._data = self._data_dependencies

    def _generate_data(self, upto: int):
        """
        Generate samples from the model up to a specific time
        :param upto: the period to sample up to
        :return: the last sample
        """
        # pre-allocate some zeros up to the length requested
        if len(self._data) < upto:
            self._data.extend([0 for _ in range(upto - len(self._data) + 1)])

        period = 0
        last_percent = 0.0
        while self._arrival_time < upto + 1:
            p = np.random.random()

            # get the rate for the current time
            rate = self.rate_at(self._arrival_time)

            # using the inverse CDF of the poisson distribution
            # this algorithm is slow for small rates. consider
            # using accept/reject in a future release
            if rate == 0:
                self._arrival_time += 1.0 / self._bucket_size_m
                continue
            else:
                _inter_arrival_time = -np.log(1.0 - p) / rate
                self._arrival_time += _inter_arrival_time

            next_period = int(np.floor(self._arrival_time))
            if next_period != period:
                new_percent = 100 * period / (upto + 1)
                if int(new_percent) != int(last_percent):
                    logger.debug(
                        "%3d percent complete on %s" % (int(new_percent), self.name)
                    )
                last_percent = new_percent
            period = next_period

            # accommodate overshoot
            if period >= len(self._data):
                self._data.extend([0 for _ in range(period - len(self._data) + 1)])

            self._data[period] += 1

        logger.debug("%3d percent complete on %s" % (100, self.name))
        self._last_updated = period - 1

        return self._data[upto]

    def __repr__(self):
        return f'TimeSeriesModel(name="{self.name}")'

    def __getitem__(self, item):
        """
        This class acts as a list - you can request any future sample from it and it will generate sample data
        :param item:
        :return: the sample requested
        """
        if self._last_updated < item:
            result = self._generate_data(upto=item)
        else:
            result = self._data[item]
        return result

    def plot(self):  # pragma: no cover
        """
        Plots the cumulative events and events by interval to visualize this item's demand
        :return:
        """
        fig, (bx, cx) = plt.subplots(2, 1, sharex="none")

        x = [self.start + (i * self.delta) for i in range(len(self._data) - 1)]
        y = np.cumsum(self._data[:-1])

        bxtick_locator = AutoDateLocator()
        bxtick_formatter = AutoDateFormatter(bxtick_locator)

        bx.xaxis.set_major_locator(bxtick_locator)
        bx.xaxis.set_major_formatter(bxtick_formatter)
        bx.plot(date2num(x), y)

        bx.set_title(
            f"Cumulative Events {self.name} ({', '.join([f'{k}: {v}' for k, v in self.dimensions.items()])})"
        )
        bx.set_ylabel("Events")
        bx.set_xlabel("Time")

        cxtick_locator = AutoDateLocator()
        cxtick_formatter = AutoDateFormatter(cxtick_locator)

        cx.xaxis.set_major_locator(cxtick_locator)
        cx.xaxis.set_major_formatter(cxtick_formatter)
        cx.plot(date2num(x), self._data[:-1])

        cx.set_title("Events by Interval")
        cx.set_ylabel("Events")
        cx.set_xlabel("Time")

        fig.autofmt_xdate()
        fig.tight_layout()

        fig.show()

    def interval_date(self, interval=0):
        """
        Get the date for a specific interval (period)
        :param interval: the interval to get the date for
        :return: the datetime at the start of the period specified
        """
        date = self.start + interval * self.delta
        return date

    def export(self):
        """
        Export the item data to OUTPUT_TIMESERIES_FILE
        :return: None
        """
        ts_file = open(OUTPUT_TIMESERIES_FILE, "a+", newline="")

        dimensions = OrderedDict(sorted(self.dimensions.items(), key=lambda t: t[0]))
        dimensions_values = [t[1] for t in dimensions.items()]

        try:
            ts_writer = csv.writer(
                ts_file, quoting=csv.QUOTE_MINIMAL, dialect=csv.unix_dialect
            )
            for idx, data in enumerate(self._data[: self._last_updated]):
                row = [self.name, self.interval_date(idx), data]
                row.extend(dimensions_values)
                ts_writer.writerow(row)
        finally:
            ts_file.close()


def get_parameter(dictionary, name, default=None):
    """
    Get a configuration parameter
    :param dictionary: the dictionary to search for the parameter
    :param name: the name of the parameter
    :param default: the default value of the parameter if not found
    :return: the value of the parameter
    """
    parameter = dictionary.get(name, default)
    if parameter is None:
        raise click.BadParameter(f"could not find required parameter {name}")
    return parameter


@click.command()
@click.option(
    "--start",
    default="2000-01-01 00:00:00",
    callback=validate_date,
    help="start date or time, formatted as YYYY-MM-DD or YYYY-MM-DD HH:MM:SS",
)
@click.option(
    "--length",
    default=100,
    type=click.IntRange(min=1),
    help="number of periods to output for each model defined in the input configuration file",
)
@click.option(
    "--plot", is_flag=True, help="set this flag to output plots of each model"
)
@click.argument("input", default="config.yaml", type=click.File("rb"))
def create(start: datetime, length: int, plot: bool, input: click.File):
    """Create synthetic data for the items defined in INPUT (default: `config.yaml`)"""

    # load the file
    config = yaml.safe_load(input)

    models = []

    # get the output frequency
    cfg_output = get_parameter(config, "output")
    cfg_output = validate_frequency(None, None, cfg_output)

    # create the models
    cfg_models = config.get("models")

    # get all dimension names
    required_dimensions = list(
        set(
            [
                item.get("name")
                for sublist in [model.get("dimensions", []) for model in cfg_models]
                for item in sublist
            ]
        )
    )

    for cfg_model in cfg_models:
        name = get_parameter(cfg_model, "name")
        rate = get_parameter(cfg_model, "rate")
        metadata = get_parameter(cfg_model, "metadata", [])
        per = validate_frequency(None, None, get_parameter(cfg_model, "per"))

        model = TimeSeriesModel(
            name=name, start=start, rate=rate, per=per, output=cfg_output
        )

        dimensions = get_parameter(cfg_model, "dimensions", [])
        for required_dimension_name in required_dimensions:
            dimension_values = [
                d.get("value")
                for d in dimensions
                if d.get("name") == required_dimension_name
            ]
            dimension_value = "unknown"
            if len(dimension_values) > 1:
                raise ValueError(f"duplicate dimensions found for model {name}")
            if len(dimension_values) == 1:
                dimension_value = next(iter(dimension_values))
            model.set_dimension(required_dimension_name, dimension_value)

        hourly_seasonality = cfg_model.get("seasonalities", {}).get("hourly", None)
        if hourly_seasonality:
            model.set_hourly_seasonalities(hourly_seasonality)

        daily_seasonality = cfg_model.get("seasonalities", {}).get("daily", None)
        if daily_seasonality:
            model.set_daily_seasonalities(daily_seasonality)

        monthly_seasonality = cfg_model.get("seasonalities", {}).get("monthly", None)
        if monthly_seasonality:
            model.set_monthly_seasonalities(monthly_seasonality)

        # add the metadata
        for metadata_item in metadata:
            name = get_parameter(metadata_item, "name")
            value = get_parameter(metadata_item, "value")
            min = get_parameter(metadata_item, "min", 1.0)
            max = get_parameter(metadata_item, "max", 1.0)

            logger.info(
                f"a adding metadata {name}: {value} (min: {min}, max: {max}) to {model.name}"
            )
            model.add_metadata(name, value, min, max)

        models.append(model)

    # add the dependencies
    for idx, cfg_model in enumerate(cfg_models):
        parent = models[idx]
        dependencies = get_parameter(cfg_model, "dependencies", [])

        for dependency in dependencies:
            dependency_model = [
                m
                for m in models
                if m.name == dependency and parent.dimensions == m.dimensions
            ]
            if len(dependency_model) == 1:
                dependency_model = dependency_model[0]
            else:
                raise ValueError(
                    f"found multiple models with name {dependency} and dimensions {parent.dimensions}"
                )

            chance = get_parameter(dependencies[dependency], "chance")
            influences_zero_rate = get_parameter(
                dependencies, "influences_zero_rate", False
            )

            parent.add_dependency(
                dependency_model, chance, influences_zero_rate=influences_zero_rate
            )

    md_export = MetadataExport()

    logger.info(f"Requesting {length + 1} time series values from all models")
    for model in models:
        logger.info(f"starting with model {model.name}")
        last_value = model[length]
        logger.info(
            f"last value for {model.name} this model is {model.interval_date(length)},{last_value}"
        )

    logger.info(f"calculating dependencies for all models")
    for model in models:
        model.calculate_dependencies()

    logger.info(f"finalizing dependencies for all models")
    for model in models:
        model.finalize_dependencies()

    for model in models:
        if plot:
            logger.info(f"plotting model {model.name}")
            model.plot()

        logger.info(f"exporting model {model.name} to csv")
        model.export()

        for metadata_item in model.metadata:
            md_export.add(model.name, metadata_item)

    logger.info("recording metadata for all models")
    md_export.export()


if __name__ == "__main__":
    create()
