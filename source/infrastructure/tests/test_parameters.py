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
import builtins
import os
from tempfile import TemporaryDirectory

import constructs
import jsii
import pytest
from aws_cdk.aws_lambda import (
    Function,
    FunctionProps,
    Code,
    Runtime,
    LayerVersionProps,
    LayerVersion,
)
from aws_cdk.core import App

from forecast.stack import ForecastStack
from interfaces import SolutionStackSubstitions


def mock_lambda_init(
    self,
    scope: constructs.Construct,
    id: builtins.str,
    *,
    code: Code,
    handler: builtins.str,
    runtime: Runtime,
    **kwargs
) -> None:
    # overriding the code will prevent building with docker (a long running operation)
    # override the runtime for now, as well, to support inline code
    props = FunctionProps(
        code=Code.from_inline("return"),
        handler=handler,
        runtime=Runtime.PYTHON_3_7,
        **kwargs
    )
    jsii.create(Function, self, [scope, id, props])


def mock_layer_init(
    self, scope: constructs.Construct, id: builtins.str, *, code: Code, **kwargs
) -> None:
    # overriding the layers will prevent building with docker (a long running operation)
    # override the runtime list for now, as well, to match above
    with TemporaryDirectory() as tmpdirname:
        kwargs["code"] = Code.from_asset(path=tmpdirname)
        kwargs["compatible_runtimes"] = [Runtime.PYTHON_3_7]
        props = LayerVersionProps(**kwargs)
        jsii.create(LayerVersion, self, [scope, id, props])


@pytest.fixture(scope="session", autouse=True)
def environment():
    os.environ.update(
        {
            "BUCKET_NAME": "test",
            "SOLUTION_NAME": "Improving Forecast Accuracy With Machine Learning",
            "QUICKSIGHT_SOURCE": "none",
            "VERSION": "1.3.3",
        }
    )


@pytest.fixture(scope="session")
def synthesis_cdk(session_mocker):
    """This currently uses the legacy stack synthesizer for speed."""
    session_mocker.patch("aws_cdk.aws_lambda.Function.__init__", mock_lambda_init)
    session_mocker.patch("aws_cdk.aws_lambda.LayerVersion.__init__", mock_layer_init)

    app = App(
        runtime_info=False,
        stack_traces=False,
        tree_metadata=False,
        analytics_reporting=False,
        context={
            "BUCKET_NAME": "test",
            "SOLUTION_NAME": "Improving Forecast Accuracy With Machine Learning",
            "QUICKSIGHT_SOURCE": "none",
            "NOTEBOOKS": "none",
            "VERSION": "1.3.3",
        },
    )
    synthesizer = SolutionStackSubstitions(qualifier="hnb659fds")
    stack = ForecastStack(app, "test", synthesizer=synthesizer)
    root = stack.node.root
    return root.synth(force=True)


@pytest.fixture(scope="session")
def synthesis_solutions(session_mocker):
    """This currently uses the legacy stack synthesizer for speed."""
    session_mocker.patch("aws_cdk.aws_lambda.Function.__init__", mock_lambda_init)
    session_mocker.patch("aws_cdk.aws_lambda.LayerVersion.__init__", mock_layer_init)

    app = App(
        runtime_info=False,
        stack_traces=False,
        tree_metadata=False,
        analytics_reporting=False,
        context={
            "BUCKET_NAME": "test",
            "SOLUTION_NAME": "Improving Forecast Accuracy With Machine Learning",
            "QUICKSIGHT_SOURCE": "none",
            "NOTEBOOKS": "none",
            "VERSION": "1.3.3",
            "SOLUTIONS_ASSETS_REGIONAL": "assets-regional",
            "SOLUTIONS_ASSETS_GLOBAL": "assets-global",
        },
    )
    synthesizer = SolutionStackSubstitions(qualifier="hnb659fds")
    stack = ForecastStack(app, "test", synthesizer=synthesizer)
    root = stack.node.root
    return root.synth(force=True)


@pytest.fixture(scope="session")
def template_cdk(synthesis_cdk):
    return synthesis_cdk.stacks[0].template


@pytest.fixture(scope="session")
def templates_cdk(synthesis_cdk):
    return synthesis_cdk.stacks


@pytest.fixture(scope="session")
def template_solutions(synthesis_solutions):
    return synthesis_solutions.stacks[0].template


@pytest.fixture(scope="session")
def templates_solutions(synthesis_solutions):
    return synthesis_solutions.stacks


REQUIRED_PARAMETERS = [
    "Email",
    "LambdaLogLevel",
    "NotebookDeploy",
    "NotebookVolumeSize",
    "NotebookInstanceType",
    "QuickSightAnalysisOwner",
]


@pytest.mark.parametrize("param_name", REQUIRED_PARAMETERS)
def test_parameters(template_cdk, param_name, templates_cdk):
    # these parameters are found, and each has a description
    assert param_name in template_cdk["Parameters"]
    assert template_cdk["Parameters"][param_name][
        "Description"
    ]  # parameter must have a description


def test_no_new_parameters(template_cdk):
    # ensure users updating the parameters also update the tests
    non_asset_parameters = [
        parameter
        for parameter in template_cdk["Parameters"]
        if "AssetParameters" not in parameter and "BootstrapVersion" not in parameter
    ]
    assert len(non_asset_parameters) == len(REQUIRED_PARAMETERS)


def test_solution_mappings(template_cdk):
    # check the required properties for AWS Solutions Deployment
    solution_data_mappings = template_cdk["Mappings"]["Solution"]["Data"]
    solution_source_mappings = template_cdk["Mappings"]["SourceCode"]["General"]
    assert solution_data_mappings["ID"] == "SO0123"
    assert solution_data_mappings["Version"]
    assert solution_data_mappings["SendAnonymousUsageData"] == "Yes"
    assert solution_source_mappings["S3Bucket"]
    assert solution_source_mappings["KeyPrefix"]
    assert solution_source_mappings["QuickSightSourceTemplateArn"]


def test_metadata(template_cdk):
    # each parameter must have a label and group
    interface = template_cdk["Metadata"]["AWS::CloudFormation::Interface"]
    assert len(interface["ParameterLabels"]) == len(REQUIRED_PARAMETERS)


def test_stack_description(template_cdk):
    assert template_cdk["Description"].startswith(
        "(SO0123) Improving Forecast Accuracy with Machine Learning"
    )


def test_notebook_bucket_cdk(template_cdk):
    tags = template_cdk["Resources"]["NotebookInstance"]["Properties"]["Tags"]
    notebook_bucket_tag = [
        tag["Value"] for tag in tags if tag["Key"] == "NOTEBOOK_BUCKET"
    ][0]
    assert notebook_bucket_tag == {"Fn::Base64": {"Ref": "ForecastBucket"}}


def test_notebook_bucket_solutions(template_solutions):
    tags = template_solutions["Resources"]["NotebookInstance"]["Properties"]["Tags"]
    notebook_bucket_tag = [
        tag["Value"] for tag in tags if tag["Key"] == "NOTEBOOK_BUCKET"
    ][0]
    assert notebook_bucket_tag == {
        "Fn::Base64": {
            "Fn::Sub": [
                "${bucket}-${region}",
                {
                    "bucket": {"Fn::FindInMap": ["SourceCode", "General", "S3Bucket"]},
                    "region": {"Ref": "AWS::Region"},
                },
            ]
        }
    }


def test_notebook_prefix_cdk(template_cdk):
    tags = template_cdk["Resources"]["NotebookInstance"]["Properties"]["Tags"]
    notebook_prefix_tag = [
        tag["Value"] for tag in tags if tag["Key"] == "NOTEBOOK_PREFIX"
    ][0]
    assert notebook_prefix_tag == {"Fn::Base64": "notebooks"}


def test_notebook_prefix_solutions(template_solutions):
    tags = template_solutions["Resources"]["NotebookInstance"]["Properties"]["Tags"]
    notebook_prefix_tag = [
        tag["Value"] for tag in tags if tag["Key"] == "NOTEBOOK_PREFIX"
    ][0]
    assert notebook_prefix_tag == {
        "Fn::Base64": {
            "Fn::Sub": [
                "${prefix}/notebooks",
                {"prefix": {"Fn::FindInMap": ["SourceCode", "General", "KeyPrefix"]}},
            ]
        }
    }
