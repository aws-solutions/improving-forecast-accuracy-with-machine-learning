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
import logging
import shutil
import subprocess
from pathlib import Path
from typing import Union, Dict, Optional

import jsii
from aws_cdk.core import ILocalBundling, BundlingOptions

from aws_solutions.cdk.helpers import copytree

logger = logging.getLogger("cdk-helper")


class UnsupportedBuildEnvironment(Exception):
    pass


@jsii.implements(ILocalBundling)
class SolutionsJavaBundling:
    """This interface allows AWS Solutions to package lambda functions for Java without the use of Docker"""

    def __init__(
        self,
        to_bundle: Path,
        gradle_task: str,
        distribution_path: Path,
        gradle_test: Optional[str] = None,
    ):
        self.to_bundle = to_bundle
        self.gradle_task = gradle_task
        self.gradle_test = gradle_test
        self.distribution_path = distribution_path

    def try_bundle(self, output_dir: str, options: BundlingOptions) -> bool:
        source = Path(self.to_bundle).absolute()

        is_gradle_build = (source / "gradlew").exists()
        if not is_gradle_build:
            raise UnsupportedBuildEnvironment("please use a gradle project")

        # Run Tests
        if self.gradle_test:
            self._invoke_local_command(
                name="gradle",
                command=["./gradlew", self.gradle_test],
                cwd=source,
            )

        # Run Build
        self._invoke_local_command(
            name="gradle",
            command=["./gradlew", self.gradle_task],
            cwd=source,
        )

        # if the distribution path is a path - it should only contain one jar or zip
        if self.distribution_path.is_dir():
            children = [child for child in self.distribution_path.iterdir()]
            if len(children) != 1:
                raise ValueError(
                    "if the distribution path is a path it should only contain one jar or zip file"
                )
            if children[0].suffix not in (".jar", ".zip"):
                raise ValueError(
                    "the distribution path does not include a single .jar or .zip file"
                )
            copytree(self.distribution_path, output_dir)
        elif self.distribution_path.is_file():
            suffix = self.distribution_path.suffix
            if suffix not in (".jar", ".zip"):
                raise ValueError("the distribution file is not a .zip or .jar file")
            shutil.copy(self.distribution_path, output_dir)

        return True

    def _invoke_local_command(
        self,
        name,
        command,
        env: Union[Dict, None] = None,
        cwd: Union[str, Path, None] = None,
        return_stdout: bool = False,
    ):

        cwd = Path(cwd)
        rv = ""

        with subprocess.Popen(
            command,
            shell=False,
            stdout=subprocess.PIPE,
            universal_newlines=True,
            cwd=cwd,
            env=env,
        ) as p:
            for line in p.stdout:
                logger.info("%s %s: %s" % (self.to_bundle.name, name, line.rstrip()))
                if return_stdout:
                    rv += line

        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, p.args)

        return rv.strip()
