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
import os
import shutil
import subprocess
from pathlib import Path

import jsii
from aws_cdk.core import ILocalBundling, BundlingOptions

logger = logging.getLogger("cdk-helper")


def copytree(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)


@jsii.implements(ILocalBundling)
class SolutionBundling:
    """This interface allows AWS Solutions to package lambda functions without the use of Docker"""

    def __init__(self, source_path, to_bundle, libs, install_path):
        self.source_path = source_path
        self.to_bundle = to_bundle
        self.libs = libs
        self.install_path = install_path

    def try_bundle(self, output_dir: str, options: BundlingOptions) -> bool:
        source = Path(self.source_path).joinpath(self.to_bundle)
        requirements_path = Path(output_dir, "requirements.txt")

        # copy source and libs
        copytree(source, output_dir)
        for lib in self.libs:
            lib_source = Path(self.source_path).joinpath(lib)
            lib_dest = Path(output_dir).joinpath(lib)
            shutil.copytree(lib_source, lib_dest)

        # install any discovered requirements
        if requirements_path.exists():
            requirements_build_path = Path(output_dir).joinpath(self.install_path)
            with subprocess.Popen(
                [
                    "pip",
                    "install",
                    "-t",
                    requirements_build_path,
                    "-r",
                    str(requirements_path),
                    "--no-color",
                ],
                shell=False,
                stdout=subprocess.PIPE,
                universal_newlines=True,
            ) as p:
                for line in p.stdout:
                    logger.info("%s pip: %s" % (self.source_path, line.strip()))
            if p.returncode != 0:
                raise subprocess.CalledProcessError(p.returncode, p.args)

        return True
