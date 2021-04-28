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
from datetime import datetime
from math import isclose

import click
import pytest
from click.testing import CliRunner
from dateutil.relativedelta import relativedelta
from flaky import flaky

from synthetic.create_synthetic_data import (
    TimeSeriesModel,
    get_parameter,
    Dependency,
    Metadata,
    validate_date,
    validate_frequency,
    MetadataExport,
    OUTPUT_METADATA_FILE,
    OUTPUT_TIMESERIES_FILE,
    create,
)


@pytest.fixture
def synth_config_params():
    return {"test": "present"}


@pytest.fixture
def model_metadata():
    return Metadata(name="my_name", value="my_value", min=5.0, max=10.0)


@pytest.fixture(scope="function")
def time_series_model():
    return TimeSeriesModel(
        name="test",
        start=datetime(2000, 1, 1),
        rate=1440.0,
        per=relativedelta(days=+1),
        output=relativedelta(days=+1),
    )


def test_time_series_output_rate(time_series_model):
    assert time_series_model.rate == 1440


def test_time_series_output_rate_weekly():
    tsm = TimeSeriesModel(
        name="test",
        start=datetime(2000, 1, 1),
        rate=1440.0,
        per=relativedelta(days=+1),
        output=relativedelta(weeks=+1),
    )

    assert tsm.rate == 1440 * 7


def test_time_series_output_rate_weekly():
    tsm = TimeSeriesModel(
        name="test",
        start=datetime(2000, 1, 1),
        rate=1440.0,
        per=relativedelta(days=+1),
        output=relativedelta(weeks=+1),
    )

    one_week_events = tsm[0]

    # one week of events should be within 5% of the expected
    assert isclose(one_week_events, 1440.0 * 7, rel_tol=0.05)


def test_get_parameter_present(synth_config_params):
    assert get_parameter(synth_config_params, "test") == "present"


def test_get_parameter_default(synth_config_params):
    assert get_parameter(synth_config_params, "not_here", "not here") == "not here"


def test_get_parameter_default(synth_config_params):
    assert get_parameter(synth_config_params, "test", "not here") != "not here"


def test_get_parameter_default(synth_config_params):
    with pytest.raises(click.BadParameter):
        assert get_parameter(synth_config_params, "not_here")


def test_dependency(mocker):
    model_mock = mocker.MagicMock()

    dep = Dependency(model=model_mock, chance=0.5, influences_zero_rate=False)

    assert dep.model == model_mock
    assert dep.chance == 0.5
    assert dep.influences_zero_rate == False


def test_metadata(model_metadata):
    assert model_metadata.name == "my_name"
    assert model_metadata.value == "my_value"
    assert model_metadata.min == 5.0
    assert model_metadata.max == 10.0


def test_metadata_repr(model_metadata):
    assert (
        repr(model_metadata)
        == 'Metadata(name="my_name", value="my_value", min=5.0, max=10.0)'
    )


def test_metadata_repr_eval(model_metadata):
    assert eval(repr(model_metadata)) == model_metadata


def test_metadata_eq(model_metadata):
    assert model_metadata != "something else"


@flaky(max_runs=100, min_passes=95)
def test_metadata_rate_distribution(model_metadata):
    # as per the module, 99.7% of the rates returned should be between min and max
    rate = model_metadata.rate
    assert rate > model_metadata.min and rate < model_metadata.max


@pytest.mark.parametrize(
    "min,max",
    [
        (10, 1),
        (10, 9),
        (0, 1),
        (1, 0),
    ],
)
def test_metadata_rate_invalid_values(model_metadata, min, max):
    with pytest.raises(ValueError):
        md = Metadata(name="a", value="b", min=min, max=max)
        _ = md.rate


@pytest.mark.parametrize(
    "date_str,dt",
    [
        ("2000-01-02", datetime(2000, 1, 2)),
        ("2000-01-02 01:02:03", datetime(2000, 1, 2, 1, 2, 3)),
    ],
)
def test_validate_date(date_str, dt):
    assert validate_date(None, None, date_str) == dt


@pytest.mark.parametrize(
    "date_str", [("2000 01 02"), ("2000/01/02"), ("2000-01-02 01:02:03Z")]
)
def test_validate_date(date_str):
    with pytest.raises(click.BadParameter):
        assert validate_date(None, None, date_str)


@pytest.mark.parametrize(
    "value,delta",
    [
        ("Y", relativedelta(years=+1)),
        ("M", relativedelta(months=+1)),
        ("W", relativedelta(weeks=+1)),
        ("D", relativedelta(days=+1)),
        ("H", relativedelta(hours=+1)),
        ("30min", relativedelta(minutes=+30)),
        ("15min", relativedelta(minutes=+15)),
        ("10min", relativedelta(minutes=+10)),
        ("5min", relativedelta(minutes=+5)),
        ("1min", relativedelta(minutes=+1)),
    ],
)
def test_validate_frequency(value, delta):
    assert validate_frequency(None, None, value) == delta


def test_invalid_frequency():
    with pytest.raises(click.BadParameter):
        validate_frequency(None, None, "30s")


def test_metadata_add_same():
    md1 = Metadata("name1", "value1")
    md2 = Metadata("name1", "value1")

    mdx = MetadataExport()
    mdx.add("item1", md1)
    mdx.add("item1", md2)

    assert len(mdx._metadata.keys()) == 1
    assert mdx._metadata["item1"]["name1"] == "value1"


def test_metadata_add_same():
    md1 = Metadata("name1", "value1")
    md2 = Metadata("name1", "value2")

    mdx = MetadataExport()
    mdx.add("item1", md1)
    with pytest.raises(ValueError):
        mdx.add("item1", md2)


def test_metadata_export():
    md_i1_1 = Metadata("name1", "value1")
    md_i1_2 = Metadata("name2", "value2")
    md_i2_1 = Metadata("name1", "value3")

    mdx = MetadataExport()
    mdx.add("item1", md_i1_1)
    mdx.add("item1", md_i1_2)
    mdx.add("item2", md_i2_1)

    with pytest.raises(ValueError):
        mdx.export()


def test_metadata_export_noop():
    mdx = MetadataExport()
    mdx.export()


def test_metadata_export_valid(mocker):
    md_i1_1 = Metadata("city", "ottawa")
    md_i1_2 = Metadata("brand", "chippies")
    md_i1_3 = Metadata("variety", "bbq")

    md_i2_1 = Metadata("city", "kanata")
    md_i2_2 = Metadata("brand", "chippies")
    md_i2_3 = Metadata("variety", "all dressed")

    mdx = MetadataExport()
    mdx.add("item1", md_i1_1)
    mdx.add("item1", md_i1_2)
    mdx.add("item1", md_i1_3)
    mdx.add("item2", md_i2_1)
    mdx.add("item2", md_i2_2)
    mdx.add("item2", md_i2_3)

    open_mock = mocker.mock_open()
    mocker.patch("synthetic.create_synthetic_data.open", open_mock, create=True)

    mdx.export()

    open_mock.assert_called_with(OUTPUT_METADATA_FILE, "a+", newline="")
    open_mock.return_value.write.assert_has_calls(
        [
            mocker.call("item1,chippies,ottawa,bbq\n"),
            mocker.call("item2,chippies,kanata,all dressed\n"),
        ],
        any_order=True,
    )


def test_time_series_add_metadata(time_series_model, model_metadata):
    time_series_model.add_metadata("my_name", "my_value", 5.0, 10.0)
    assert model_metadata in time_series_model.metadata


def test_time_series_add_dependency(time_series_model):
    time_series_model.add_dependency(time_series_model, 0.5, influences_zero_rate=False)
    assert time_series_model in [dep.model for dep in time_series_model.dependencies]


def test_time_series_set_dimension(time_series_model):
    time_series_model.set_dimension("brand", "my_brand")
    assert (
        "brand" in time_series_model.dimensions.keys()
        and time_series_model.dimensions["brand"] == "my_brand"
    )


@flaky(max_runs=10, min_passes=8)
def test_time_series_mean(time_series_model):
    last_value = time_series_model[1]
    assert isclose(time_series_model.mean, 1440.0, rel_tol=0.95)


@pytest.mark.parametrize(
    "setter,property,expected",
    [
        ("set_hourly_seasonalities", "hourly_seasonality", [1 for _ in range(24)]),
        ("set_daily_seasonalities", "daily_seasonality", [1 for _ in range(7)]),
        ("set_monthly_seasonalities", "monthly_seasonality", [1 for _ in range(12)]),
    ],
)
def test_seasonalities_valid(time_series_model, setter, property, expected):
    setter = getattr(time_series_model, setter)
    setter(expected)
    result = getattr(time_series_model, property)
    assert result == expected


@pytest.mark.parametrize(
    "setter",
    [
        ("set_hourly_seasonalities"),
        ("set_daily_seasonalities"),
        ("set_monthly_seasonalities"),
    ],
)
def test_seasonalities_valid(time_series_model, setter):
    with pytest.raises(ValueError):
        setter = getattr(time_series_model, setter)
        setter([1])


def test_metadata_adjusts_rate(time_series_model):
    assert time_series_model.rate_at(0) == 1440

    time_series_model.add_metadata("a", "b", 5.0, 10.0)
    assert time_series_model.rate_at(0) != 1440


def test_generating_data(time_series_model):
    last_sample = time_series_model[0]
    assert len(time_series_model._data) > 0


def test_dependencies_complementary():
    tsm1 = TimeSeriesModel(
        name="m1",
        start=datetime(2000, 1, 1),
        rate=15,
        per=relativedelta(hours=+1),
        output=relativedelta(hours=+1),
    )

    tsm2 = TimeSeriesModel(
        name="m2",
        start=datetime(2000, 1, 1),
        rate=10,
        per=relativedelta(hours=+1),
        output=relativedelta(hours=+1),
    )

    tsm1.add_dependency(tsm2, 0.5)

    last_tsm1 = tsm1[100]
    last_tsm2 = tsm2[100]

    pre_sum1 = sum(tsm1._data)
    pre_sum2 = sum(tsm2._data)

    for item in [tsm1, tsm2]:
        item.calculate_dependencies()

    for item in [tsm1, tsm2]:
        item.finalize_dependencies()

    post_sum1 = sum(tsm1._data)
    post_sum2 = sum(tsm2._data)

    assert pre_sum1 != post_sum1
    assert pre_sum2 == post_sum2


def test_interval_date(time_series_model):
    assert time_series_model.interval_date(0) == datetime(2000, 1, 1)
    assert time_series_model.interval_date(10) == datetime(2000, 1, 11)


def test_timeseries_export(time_series_model, mocker):
    last_period = 10
    time_series_model.set_dimension("b", "b")
    time_series_model.set_dimension("a", "a")
    _ = time_series_model[last_period]

    open_mock = mocker.mock_open()
    mocker.patch("synthetic.create_synthetic_data.open", open_mock, create=True)

    time_series_model.export()

    open_mock.assert_called_with(OUTPUT_TIMESERIES_FILE, "a+", newline="")
    open_mock.return_value.write.call_count == last_period


def test_create():
    config = """---
output: H
no_sales_at_night: &no_sales_at_night [0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,0]

models: 
  - name: CHIP-001
    rate: 60
    per: D
    dimensions: 
      - name: store
        value: ottawa
      - name: flavour
        value: all dressed
    metadata: 
      - name: brand
        value: chippy
    seasonalities: 
      hourly: *no_sales_at_night
    dependencies:
      CHIP-002:
        chance: .5
    
  - name: CHIP-002
    rate: 60
    per: D
    dimensions: 
      - name: store
        value: ottawa
      - name: flavour
        value: all dressed
    metadata: 
      - name: brand
        value: chippy
    seasonalities: 
      hourly: *no_sales_at_night
    dependencies:
      CHIP-001:
        chance: .5    

  - name: CHIP-001
    rate: 60
    per: D
    dimensions: 
      - name: store
        value: kanata
      - name: flavour
        value: all dressed
    metadata: 
      - name: brand
        value: chippy
    seasonalities: 
      hourly: *no_sales_at_night
    dependencies:
      CHIP-002:
        chance: .5
    
  - name: CHIP-002
    rate: 60
    per: D
    dimensions: 
      - name: store
        value: kanata
      - name: flavour
        value: all dressed
    metadata: 
      - name: brand
        value: chippy
    seasonalities: 
      hourly: *no_sales_at_night
    dependencies:
      CHIP-002:
        chance: .5   
    """

    runner = CliRunner()
    with runner.isolated_filesystem():
        result = runner.invoke(create, args="-", input=config, catch_exceptions=False)

    assert result.exit_code == 0
    print("HI")
