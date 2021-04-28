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

from moto import mock_sts

from lambdas.createquicksightanalysis.handler import createquicksightanalysis


@mock_sts
def test_prepare_export(sfn_configuration_data, mocker):
    config_mock = mocker.MagicMock()
    mocker.patch("lambdas.createquicksightanalysis.handler.QuickSight", config_mock)

    createquicksightanalysis(sfn_configuration_data, None)

    assert config_mock.called
    assert config_mock.return_value.create_data_source.called
    assert config_mock.return_value.create_data_set.called
    assert config_mock.return_value.create_analysis.called
