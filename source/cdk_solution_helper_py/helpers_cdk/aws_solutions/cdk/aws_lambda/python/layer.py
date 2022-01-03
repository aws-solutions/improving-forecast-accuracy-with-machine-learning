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

from pathlib import Path
from typing import Union, List
from uuid import uuid4

from aws_cdk.aws_lambda import LayerVersion, Code
from aws_cdk.core import Construct, BundlingOptions, BundlingDockerImage, AssetHashType

from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonBundling

DEPENDENCY_EXCLUDES = ["*.pyc"]


class SolutionsPythonLayerVersion(LayerVersion):
    """Handle local packaging of layer versions"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        requirements_path: Path,
        libraries: Union[List[Path], None] = None,
        **kwargs,
    ):  # NOSONAR
        self.scope = scope
        self.construct_id = construct_id
        self.requirements_path = requirements_path

        # validate requirements path
        if not self.requirements_path.is_dir():
            raise ValueError(
                f"requirements_path {self.requirements_path} must not be a file, but rather a directory containing Python requirements in a requirements.txt file, pipenv format or poetry format"
            )

        libraries = [] if not libraries else libraries
        for lib in libraries:
            if lib.is_file():
                raise ValueError(
                    f"library {lib} must not be a file, but rather a directory"
                )

        bundling = SolutionsPythonBundling(
            self.requirements_path, libraries=libraries, install_path="python"
        )

        kwargs["code"] = self._get_code(bundling)

        # initialize the LayerVersion
        super().__init__(scope, construct_id, **kwargs)

    def _get_code(self, bundling: SolutionsPythonBundling) -> Code:
        # create the layer version locally
        code_parameters = {
            "path": str(self.requirements_path),
            "asset_hash_type": AssetHashType.CUSTOM,
            "asset_hash": uuid4().hex,
            "exclude": DEPENDENCY_EXCLUDES,
        }

        code = Code.from_asset(
            bundling=BundlingOptions(
                image=BundlingDockerImage.from_registry(
                    "scratch"
                ),  # NEVER USED - FOR NOW ALL BUNDLING IS LOCAL
                command=["not_used"],
                entrypoint=["not_used"],
                local=bundling,
            ),
            **code_parameters,
        )

        return code
