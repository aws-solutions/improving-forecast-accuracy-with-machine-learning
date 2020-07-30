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

from lambdas.createforecast.handler import createforecast
from shared.helpers import ResourceInvalid
from shared.status import Status


def test_create_forecast(sfn_configuration_data, mocker):
    status_override = Status.DOES_NOT_EXIST

    class MockForecast:
        @property
        def status(self):
            return status_override

        def create(self):
            pass

        @property
        def arn(self):
            return "arn:aws:forecast:::forecast/forecast-id"

    class MockConfig:
        @classmethod
        def from_sfn(cls, event):
            return MockConfig()

        def forecast(self, *args, **kwargs):
            return MockForecast()

    mocker.patch("lambdas.createforecast.handler.Config", MockConfig)

    with pytest.raises(ResourceInvalid):
        arn = createforecast(sfn_configuration_data, None)
