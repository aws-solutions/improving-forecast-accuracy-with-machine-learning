# #####################################################################################################################
#  Copyright 2020-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                       #
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

import datetime

from lambdas.creategluetablename.handler import creategluetablename


def test_create_table_name(mocker, monkeypatch):
    datetime_mock = mocker.MagicMock(wraps=datetime.datetime)
    datetime_mock.now.return_value = datetime.datetime(2000, 1, 2, 3, 4, 5)
    monkeypatch.setattr("lambdas.creategluetablename.handler.datetime", datetime_mock)

    evt = {"dataset_group_name": "testing_123"}

    assert creategluetablename(evt, None) == "testing_123_2000_01_02_03_04_05"
