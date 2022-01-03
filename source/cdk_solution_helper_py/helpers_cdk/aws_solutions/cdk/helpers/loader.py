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

import importlib
import json
import logging
from functools import wraps
from pathlib import Path

logger = logging.getLogger("cdk-helper")


class CDKLoaderException(Exception):
    pass


def log_error(error):
    logger.error(error)
    raise CDKLoaderException(error)


def _cdk_json_present(func):
    @wraps(func)
    def cdk_json_present(cdk_app_path: Path, cdk_app_name):
        app_path = cdk_app_path.parent
        cdk_json_dict = {}
        if not Path(app_path / "cdk.json").exists():
            log_error(f"please ensure a cdk.json is present at {app_path}")

        try:
            cdk_json_dict = json.loads(Path(app_path / "cdk.json").read_text())
        except ValueError as exc:
            log_error(f"failed to parse cdk.json: {exc}")

        cdk_app = cdk_json_dict.get("app")
        if not cdk_app:
            log_error(f"failed to find `app` in cdk.json")

        if "python3" not in cdk_app:
            log_error(
                f"this helper only supports python3 CDK apps at this time - yours was declared as {cdk_app}"
            )

        return func(cdk_app_path, cdk_app_name)

    return cdk_json_present


@_cdk_json_present
def load_cdk_app(cdk_app_path, cdk_app_name):
    """
    Load a CDK app from a folder path (dynamically)
    :param cdk_app_path: The full path of the CDK app to load
    :param cdk_app_name: The module path (starting from cdk_app_path) to find the function returning synth()
    :return:
    """

    try:
        (cdk_app_name, cdk_app_entrypoint) = cdk_app_name.split(":")
    except ValueError:
        log_error("please provide your `cdk_app_name` as path.to.cdk:function_name")

    if not cdk_app_path.exists():
        log_error(f"could not find `{cdk_app_name}` (please use a full path)")

    spec = importlib.util.spec_from_file_location(cdk_app_name, cdk_app_path)
    module = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        log_error(f"could not load `{cdk_app_entrypoint}` in `{cdk_app_name}`: {exc}")

    try:
        cdk_function = getattr(module, cdk_app_entrypoint)
    except AttributeError as exc:
        log_error(
            f"could not find CDK entrypoint `{cdk_app_entrypoint}` in `{cdk_app_name}`"
        )

    logger.info(f"loaded AWS CDK app from {cdk_app_path}")
    logger.info(
        f"loaded AWS CDK app at {cdk_app_name}, entrypoint is {cdk_app_entrypoint}"
    )
    return cdk_function
