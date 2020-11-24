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
import logging
import shutil
import subprocess
from pathlib import Path

import jsii
from aws_cdk.core import ILocalBundling, BundlingOptions

logger = logging.getLogger("cdk-helper")


@jsii.implements(ILocalBundling)
class SolutionBundling:
    def __init__(self, source_path, to_bundle, libs, install_path):
        self.source_path = source_path
        self.to_bundle = to_bundle
        self.libs = libs
        self.install_path = install_path

    def try_bundle(self, output_dir: str, options: BundlingOptions) -> bool:
        source = Path(self.source_path).joinpath(self.to_bundle)
        requirements_path = Path(output_dir, "requirements.txt")

        # copy source and libs
        shutil.copytree(source, output_dir, dirs_exist_ok=True)
        for lib in self.libs:
            lib_source = Path(self.source_path).joinpath(lib)
            lib_dest = Path(output_dir).joinpath(lib)
            shutil.copytree(lib_source, lib_dest, dirs_exist_ok=True)

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
