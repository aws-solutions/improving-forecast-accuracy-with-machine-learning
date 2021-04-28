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

import grp
import logging
import os
import pwd
import subprocess

import boto3
import pytest
from botocore.stub import Stubber
from moto import mock_s3


@pytest.fixture(autouse=True)
def default_region():
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture()
def lifecycle_config():
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"
    import notebook.lifecycle_config as lifecycle_config

    yield lifecycle_config
    os.environ.pop("AWS_DEFAULT_REGION", None)


@pytest.fixture
def forecast_stub():
    client = boto3.client("sagemaker", region_name="us-east-1")
    with Stubber(client) as stubber:
        yield stubber


@pytest.fixture(scope="function")
def s3_valid_config():
    with mock_s3():
        client = boto3.client("s3", region_name=os.environ.get("AWS_REGION"))
        client.create_bucket(Bucket="testbucket")
        client.put_object(
            Bucket="testbucket", Key="some/SampleVisualization.ipynb", Body="contents"
        )
        yield client


def test_get_tag_present(lifecycle_config, mocker, forecast_stub, caplog):
    mocker.patch(
        "notebook.lifecycle_config.open",
        mocker.mock_open(read_data='{"ResourceArn": "arn::mocked"}'),
    )
    mocker.patch(
        "notebook.lifecycle_config.sagemaker_cli",
    )

    lifecycle_config.sagemaker_cli = forecast_stub.client
    forecast_stub.add_response(
        "list_tags", {"Tags": [{"Key": "tagname", "Value": "tagvalue"}]}
    )

    with caplog.at_level(logging.INFO):
        assert lifecycle_config.get_tag("tagname") == "tagvalue"

    assert "Notebook instance ARN is arn::mocked" in caplog.text
    assert "Tag tagname value is tagvalue" in caplog.text


def test_get_tag_present(lifecycle_config, mocker, forecast_stub):
    mocker.patch(
        "notebook.lifecycle_config.open",
        mocker.mock_open(read_data='{"ResourceArn": "arn::mocked"}'),
    )
    mocker.patch(
        "notebook.lifecycle_config.sagemaker_cli",
    )

    lifecycle_config.sagemaker_cli = forecast_stub.client
    forecast_stub.add_response(
        "list_tags", {"Tags": [{"Key": "tagname", "Value": "tagvalue"}]}
    )

    assert not lifecycle_config.get_tag("missing")


def test_get_tag_b64(lifecycle_config, mocker, forecast_stub):
    mocker.patch(
        "notebook.lifecycle_config.open",
        mocker.mock_open(read_data='{"ResourceArn": "arn::mocked"}'),
    )
    mocker.patch(
        "notebook.lifecycle_config.sagemaker_cli",
    )

    lifecycle_config.sagemaker_cli = forecast_stub.client
    forecast_stub.add_response(
        "list_tags", {"Tags": [{"Key": "tagname", "Value": "dGVzdA=="}]}
    )

    assert lifecycle_config.get_tag("tagname", is_base64=True) == "test"


def test_set_jupyter_env_from_tag(lifecycle_config, mocker):
    mock_open = mocker.mock_open()
    mocker.patch("notebook.lifecycle_config.get_tag", return_value="tagvalue")
    mocker.patch("notebook.lifecycle_config.open", mock_open)

    assert lifecycle_config.set_jupyter_env_from_tag("tagname") == "tagvalue"
    mock_open().write.assert_called_once_with("export tagname=tagvalue\n")


def test_clean_env_file(lifecycle_config, mocker):
    mock = mocker.MagicMock()
    mocker.patch("notebook.lifecycle_config.os.remove", mock)

    lifecycle_config.clean_env_file()

    mock.assert_called_once_with(lifecycle_config.JUPYTER_ENV_FILE)


def test_clean_env_file_missing(lifecycle_config, mocker):
    mock = mocker.MagicMock()
    mock.side_effect = FileNotFoundError()
    mocker.patch("notebook.lifecycle_config.os.remove", mock)

    lifecycle_config.clean_env_file()

    mock.assert_called_once_with(lifecycle_config.JUPYTER_ENV_FILE)


def test_restart_notebook_server(lifecycle_config, mocker, caplog):
    completed_process = subprocess.CompletedProcess(args=[], returncode=0)
    mocker.patch(
        "notebook.lifecycle_config.subprocess.run", return_value=completed_process
    )

    with caplog.at_level(logging.INFO):
        lifecycle_config.restart_notebook_server()

    assert "Restarting Jupyter Server" in caplog.text
    assert "Failed" not in caplog.text


def test_copy_files(s3_valid_config, lifecycle_config, tmpdir):
    # this test will fail on Windows machines and if the group is not resolvable (networking issues)
    uid = pwd.getpwuid(os.getuid()).pw_name
    gid = grp.getgrgid(os.getgid()).gr_name

    lifecycle_config.NOTEBOOKS = ["SampleVisualization.ipynb"]
    lifecycle_config.copy_files(
        "testbucket", "some", tmpdir.dirname, username=uid, groupname=gid
    )
    assert os.path.isfile(os.path.join(tmpdir.dirname, "SampleVisualization.ipynb"))
