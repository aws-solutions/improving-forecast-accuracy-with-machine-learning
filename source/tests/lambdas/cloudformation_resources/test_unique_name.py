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

from lambdas.cloudformation_resources.unique_name import generate_name, helper


@pytest.fixture(params=[-1, 1, 16, 32, 33])
def lambda_event(request):
    param = request.param
    event = {"Length": str(param)}
    yield event


def test_generate_name(lambda_event):
    id_generated = generate_name(lambda_event, None)

    fixture_length = int(lambda_event.get("Length"))
    if fixture_length > 32:
        desired_length = 32
    elif fixture_length < 1:
        desired_length = max(0, 32 + fixture_length)
    else:
        desired_length = fixture_length

    assert len(id_generated) == desired_length
    assert len(helper.Data["Id"]) == desired_length


def test_generate_name_invalid():
    id_generated = generate_name({"Length": "ABC"}, None)
    assert len(id_generated) == 32
