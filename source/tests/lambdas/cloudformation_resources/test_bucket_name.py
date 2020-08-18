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

from lambdas.cloudformation_resources.bucket_name import (
    get_property,
    generate_name,
    helper,
)


@pytest.fixture()
def lambda_event():
    event = {
        "ResourceProperties": {
            "Id": "UniqueId",
            "StackName": "StackName",
            "BucketPurpose": "Purpose",
        }
    }
    yield event


def test_generate_name(lambda_event):
    generate_name(lambda_event, None)

    assert helper.Data["Name"] == "stackname-purpose-uniqueid"


def test_generate_long_name(lambda_event):
    lambda_event["ResourceProperties"]["StackName"] = "a" * 63
    with pytest.raises(ValueError):
        generate_name(lambda_event, None)


def test_get_property_present(lambda_event):
    assert get_property(lambda_event, "StackName") == "StackName"


def test_get_property_default(lambda_event):
    assert get_property(lambda_event, "MissingProperty", "DEFAULT") == "DEFAULT"


def test_get_property_missing(lambda_event):
    with pytest.raises(ValueError):
        get_property(lambda_event, "MissingProperty")
