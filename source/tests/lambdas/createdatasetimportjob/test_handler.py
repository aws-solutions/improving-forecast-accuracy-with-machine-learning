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

from lambdas.createdatasetimportjob.handler import createdatasetimportjob
from shared.status import Status


@mock_sts
def test_createdatasetimportjob(sfn_configuration_data, mocker):
    sfn_configuration_data["dataset_file"] = "train/RetailDemandTRMProphet.csv"

    dataset_import_job_mock = mocker.MagicMock()
    type(dataset_import_job_mock.return_value).status = mocker.PropertyMock(
        side_effect=[Status.DOES_NOT_EXIST, Status.ACTIVE]
    )
    mocker.patch(
        "lambdas.createdatasetimportjob.handler.Config.dataset_import_job",
        dataset_import_job_mock,
    )

    createdatasetimportjob(sfn_configuration_data, None)
    assert dataset_import_job_mock.return_value.create.called
