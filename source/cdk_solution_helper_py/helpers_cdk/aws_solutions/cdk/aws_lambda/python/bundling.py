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

import importlib.util
import logging
import os
import platform
import subprocess
from pathlib import Path
from typing import Dict, Union

import jsii
from aws_cdk.aws_lambda import Runtime
from aws_cdk.core import ILocalBundling, BundlingOptions

from aws_solutions.cdk.helpers import copytree

DEFAULT_RUNTIME = Runtime.PYTHON_3_7
BUNDLER_DEPENDENCIES_CACHE = "/var/dependencies"
REQUIREMENTS_TXT_FILE = "requirements.txt"
REQUIREMENTS_PIPENV_FILE = "Pipfile"
REQUIREMENTS_POETRY_FILE = "pyproject.toml"


logger = logging.getLogger("cdk-helper")


class SolutionsPythonBundlingException(Exception):
    pass


@jsii.implements(ILocalBundling)
class SolutionsPythonBundling:
    """This interface allows AWS Solutions to package lambda functions without the use of Docker"""

    def __init__(self, to_bundle, libraries, install_path=""):
        self.to_bundle = to_bundle
        self.libraries = libraries
        self.install_path = install_path

    @property
    def platform_supports_bundling(self):
        os_platform = platform.system()
        os_platform_can_bundle = os_platform in ["Darwin", "Linux"]
        logger.info(
            "local bundling %s supported for %s"
            % ("is" if os_platform_can_bundle else "is not", os_platform)
        )
        return os_platform_can_bundle

    def try_bundle(self, output_dir: str, options: BundlingOptions) -> bool:
        if not self.platform_supports_bundling:
            raise SolutionsPythonBundlingException(
                "this platform does not support bundling"
            )

        source = Path(self.to_bundle).absolute()

        # copy source
        copytree(source, output_dir)

        # copy libraries
        for lib in self.libraries:
            lib_source = Path(lib).absolute()
            lib_dest = Path(output_dir).joinpath(lib.name)
            copytree(lib_source, lib_dest)

        try:
            self._local_bundle_with_poetry(output_dir)
            self._local_bundle_with_pipenv(output_dir)
            self._local_bundle_with_pip(output_dir)
        except subprocess.CalledProcessError as cpe:
            raise SolutionsPythonBundlingException(
                f"local bundling was tried but failed: {cpe}"
            )

        return True

    def _invoke_local_command(
        self,
        name,
        command,
        save_stdout: Path = None,
        env: Union[Dict, None] = None,
        cwd: Union[str, Path, None] = None,
    ):
        if save_stdout and save_stdout.exists():
            raise SolutionsPythonBundlingException(
                f"{save_stdout} already exists - abandoning"
            )

        if save_stdout:
            save_file = open(save_stdout, "w")
        else:
            save_file = None

        cwd = Path(cwd)

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
                if save_file:
                    save_file.write(line)

        if save_file:
            save_file.close()

        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, p.args)

    def validate_requirements_file(self, output_dir):
        requirements_file = Path(output_dir) / REQUIREMENTS_TXT_FILE
        with open(requirements_file, "r") as requirements:
            for requirement in requirements:
                if requirement.lstrip().startswith("-e"):
                    raise SolutionsPythonBundlingException(
                        "ensure no requirements are flagged as editable. if editable requirements are required, break down your requirements into requirements.txt and requirements-dev.txt"
                    )

    def _source_file_exists(self, name, output_dir):
        source_file = Path(output_dir) / name
        exists = source_file.exists()
        logger.info("%s file %s found" % (name, "was" if exists else "was not"))
        return exists

    def _required_package_exists(self, package):
        if not importlib.util.find_spec(package):
            missing_package = (
                f"required package {package} was not installed - please install it"
            )
            logger.warning(missing_package)
            raise SolutionsPythonBundlingException(missing_package)
        return True

    def _local_bundle_with_pip(self, output_dir):
        if not self._source_file_exists(REQUIREMENTS_TXT_FILE, output_dir):
            logger.info("no pip bundling to perform")
            return

        self._required_package_exists("pip")
        self.validate_requirements_file(output_dir)

        requirements_build_path = Path(output_dir).joinpath(self.install_path)
        command = [
            "pip",
            "install",
            "-t",
            str(requirements_build_path),
            "-r",
            str(Path(output_dir) / REQUIREMENTS_TXT_FILE),
        ]
        self._invoke_local_command("pip", command, cwd=self.to_bundle)

    def _local_bundle_with_pipenv(self, output_dir):
        if not self._source_file_exists(REQUIREMENTS_PIPENV_FILE, output_dir):
            return  # no Pipenv file found - do not bundle with Pipenv

        if self._source_file_exists(REQUIREMENTS_TXT_FILE, output_dir):
            logger.error(
                "both a Pipenv and requirements.txt file were found - use one or the other"
            )
            raise SolutionsPythonBundlingException(
                "confusing Python package bundling - use one of requirements.txt (pip), pipenv (Pipenv) or pyproject.toml (poetry)"
            )

        self._required_package_exists("pipenv")

        command = ["pipenv", "--bare", "lock", "--no-header", "-r"]
        env = os.environ.copy()
        env.update(
            {
                "PIPENV_VERBOSITY": "-1",
                "PIPENV_CLEAR": "true",
            }
        )
        self._invoke_local_command(
            "pipenv",
            command,
            save_stdout=Path(output_dir) / REQUIREMENTS_TXT_FILE,
            env=env,
            cwd=output_dir,
        )

    def _local_bundle_with_poetry(self, output_dir):
        if not self._source_file_exists(REQUIREMENTS_POETRY_FILE, output_dir):
            return  # no pyproject.toml file found - do not bundle with poetry

        if self._source_file_exists(REQUIREMENTS_TXT_FILE, output_dir):
            logger.error(
                "both a pyproject.toml and requirements.txt file were found - use one or the other"
            )
            raise SolutionsPythonBundlingException(
                "confusing Python package bundling - use one of requirements.txt (pip), pipenv (Pipenv) or pyproject.toml (poetry)"
            )

        self._required_package_exists("poetry")

        command = [
            "poetry",
            "export",
            "--with-credentials",
            "--format",
            REQUIREMENTS_TXT_FILE,
            "--output",
            str(Path(output_dir) / REQUIREMENTS_TXT_FILE),
        ]
        self._invoke_local_command("poetry", command, cwd=output_dir)
