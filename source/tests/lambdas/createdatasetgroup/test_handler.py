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

from moto import mock_sts

from lambdas.createdatasetgroup.handler import createdatasetgroup
from shared.status import Status


@mock_sts
def test_create_forecast_active(sfn_configuration_data, mocker):
    sfn_configuration_data["dataset_file"] = "train/RetailDemandTRMProphet.csv"

    datasets_mock = mocker.MagicMock()
    datasets_mock.return_value = [mocker.MagicMock() for i in range(3)]
    dataset_groups_mock = mocker.MagicMock()
    dataset_groups_mock.return_value = [mocker.MagicMock() for i in range(3)]
    mocker.patch("lambdas.createdatasetgroup.handler.Config.datasets", datasets_mock)
    mocker.patch(
        "lambdas.createdatasetgroup.handler.Config.dataset_groups", dataset_groups_mock
    )

    for dataset_group in dataset_groups_mock.return_value:
        type(dataset_group).status = mocker.PropertyMock(
            side_effect=[Status.DOES_NOT_EXIST, Status.ACTIVE]
        )

    createdatasetgroup(sfn_configuration_data, None)

    assert all(dsg.update.called for dsg in dataset_groups_mock.return_value)
    assert all(dsg.create.called for dsg in dataset_groups_mock.return_value)
