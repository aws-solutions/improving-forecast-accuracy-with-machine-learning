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

import pytest

from lambdas.createforecast.create_forecast import handler as create_forecast
from lambdas.createforecast.create_forecast_export import (
    handler as create_forecast_export,
)
from shared.helpers import ResourceInvalid
from shared.status import Status


@pytest.fixture(params=[Status.DOES_NOT_EXIST, Status.ACTIVE])
def mock_config(request, mocker):
    class MockForecast:
        @property
        def status(self):
            return request.param

        def create(self):
            pass

        def export(self, dataset_file):
            _mocker = mocker.MagicMock()
            type(_mocker).status = mocker.PropertyMock(return_value=Status.ACTIVE)
            return _mocker

        @property
        def arn(self):
            return "arn:aws:forecast:::forecast/forecast-id"

    class MockConfig:
        def __init__(self):
            self._forecast = MockForecast()

        @classmethod
        def from_sfn(cls, event):
            return MockConfig()

        def forecast(self, *args, **kwargs):
            return self._forecast

    return MockConfig, request.param


def test_create_forecast(sfn_configuration_data, mock_config, mocker):
    mock_config, status = mock_config
    mocker.patch("lambdas.createforecast.create_forecast.Config", mock_config)

    if status == Status.DOES_NOT_EXIST:
        with pytest.raises(ResourceInvalid):
            create_forecast(sfn_configuration_data, None)
    else:
        result = create_forecast(sfn_configuration_data, None)
        assert result == "arn:aws:forecast:::forecast/forecast-id"


def test_create_forecast_export(sfn_configuration_data, mock_config, mocker):
    mock_config, status = mock_config
    mocker.patch("lambdas.createforecast.create_forecast_export.Config", mock_config)

    if status == Status.DOES_NOT_EXIST:
        with pytest.raises(ValueError):
            create_forecast_export(sfn_configuration_data, None)
    else:
        result = create_forecast_export(sfn_configuration_data, None)
        assert result == "arn:aws:forecast:::forecast/forecast-id"
