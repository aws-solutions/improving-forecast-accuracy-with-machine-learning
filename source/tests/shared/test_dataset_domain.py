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

from shared.Dataset.dataset_domain import DatasetDomain


def test_dataset_domain_valid():
    valid_domains = "RETAIL | CUSTOM | INVENTORY_PLANNING | EC2_CAPACITY | WORK_FORCE | WEB_TRAFFIC | METRICS".replace(
        " ", ""
    ).split(
        "|"
    )
    for domain in valid_domains:
        enum = DatasetDomain[domain]
        assert enum == domain


def test_dataset_domain_invalid():
    with pytest.raises(KeyError):
        DatasetDomain["NOT_REAL"]


def test_dataset_domain_equality():
    domain = DatasetDomain["RETAIL"]
    assert domain == "RETAIL"
    assert DatasetDomain.RETAIL == domain
