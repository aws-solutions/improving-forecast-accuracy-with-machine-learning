#!/usr/bin/env python3
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
import os
import shutil
import subprocess
from uuid import uuid4 as uuid

import click
from jinja2 import Environment, BaseLoader, FileSystemLoader, ChoiceLoader

# logging configuration
logger = logging.getLogger("package-cloudformation")
handler = logging.StreamHandler()
formatter = logging.Formatter("[%(levelname)s]\t%(name)s\t%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class Cleaner:
    """Encapsulates functions that help clean up the build environment."""

    TO_CLEAN = [
        {"name": "Python bytecode", "file_type": "f", "pattern": "*.py[cod]"},
        {"name": "Python Coverage databases", "file_type": "f", "pattern": ".coverage"},
        {
            "name": "Python bytecode cache directory",
            "file_type": "d",
            "pattern": "__pycache__",
        },
        {
            "name": "Python test cache",
            "pattern": ".pytest_cache",
            "file_type": "d",
            "command": ["-exec", "rm", "-rf", "{}", "+"],
        },
    ]

    def clean_dirs(self, *args):
        """Recursively remove each of its arguments, then recreate the directory"""
        for dir_to_remove in args:
            logger.info("cleaning %s" % dir_to_remove)
            shutil.rmtree(dir_to_remove, ignore_errors=True)
            os.makedirs(dir_to_remove)

    def cleanup_source(self, source_dir):
        """Uses the `find` command to clean up items listed in TO_CLEAN"""
        pwd = os.getcwd()
        os.chdir(source_dir)
        for item in Cleaner.TO_CLEAN:
            name = item.get("name")
            pattern = item.get("pattern")
            file_type = item.get("file_type")
            command = ["find", ".", "-type", file_type, "-name", pattern]
            command.extend(item.get("command", ["-delete"]))

            logger.info("cleaning up %s" % name)
            result = subprocess.run(command)
            if result.returncode != 0:
                logging.error("Failed to clean up" % name)
        os.chdir(pwd)


class BuildEnvironment:
    def __init__(
        self,
        source_bucket_name,
        solution_name,
        version_code,
        dev_mode,
        template_dir=None,
    ):
        if not template_dir:
            template_dir = os.getcwd()

        # set up build required properties
        self._source_bucket_name = source_bucket_name
        self._solution_name = solution_name
        self._version_code = version_code
        self._deployment_id = uuid().hex if dev_mode else version_code

        logger.info("build environment solution: %s" % solution_name)
        logger.info("build environment bucket: %s" % source_bucket_name)
        logger.info("build environment version: %s" % version_code)
        logger.info("build environment deployment ID: %s" % self._deployment_id)

        # set up build paths
        self._template_dir = template_dir
        self._template_dist_dir = os.path.join(template_dir, "global-s3-assets")
        self._build_dir = os.path.join(template_dir, "build-s3-assets")
        self._build_dist_dir = os.path.join(template_dir, "regional-s3-assets")
        self._source_dir = os.path.normpath(
            os.path.join(template_dir, os.pardir, "source")
        )
        self._notebook_dir = os.path.join(
            self._source_dir, "notebook", "samples", "notebooks"
        )

        logger.debug("build environment template directory: %s" % self._template_dir)
        logger.debug(
            "build environment template distribution directory: %s"
            % self._template_dist_dir
        )
        logger.debug("build environment build directory: %s" % self._build_dir)
        logger.debug(
            "build environment build distribution directory: %s" % self._build_dist_dir
        )
        logger.debug("build environment source directory: %s" % self._source_dir)
        logger.debug("build environment notebook directory: %s" % self._notebook_dir)

    def clean(self):
        """Clean up the build environment"""
        cleaner = Cleaner()
        cleaner.clean_dirs(self.template_dist_dir, self.build_dir, self.build_dist_dir)
        cleaner.cleanup_source(self.source_dir)

    @property
    def template_dir(self):
        """Path containing CloudFormation templates to build"""
        return self._template_dir

    @property
    def template_dist_dir(self):
        """Path containing built CloudFormation templates"""
        return self._template_dist_dir

    @property
    def build_dir(self):
        """Path containing artifacts being built"""
        return self._build_dir

    @property
    def build_dist_dir(self):
        """Path containing built artifacts"""
        return self._build_dist_dir

    @property
    def source_dir(self):
        """Path containing source"""
        return self._source_dir

    @property
    def notebook_dir(self):
        """Path containing Jupyter Notebooks"""
        return self._notebook_dir

    @property
    def source_bucket_name(self):
        """S3 bucket name for global assets"""
        return self._source_bucket_name

    @property
    def solution_name(self):
        """Solution name"""
        return self._solution_name

    @property
    def version_code(self):
        """Solution version code"""
        return self._version_code

    @property
    def deployment_id(self):
        """Solution deployment ID"""
        return self._deployment_id


class GlobalMacroLoader(BaseLoader):
    """This loader can be used to prepend macros globally into Jinja2 rendered CloudFormation Templates"""

    def __init__(self, path):
        self.path = path
        self.macros = []

    def add_macro(self, macro):
        """Add a macro to be used globally by templates this loader loads"""
        self.macros.append(macro)

    def get_source(self, environment, template):
        """Get the source for the template, preprended by the list of macros added"""
        (source, filename, uptodate) = self.path.get_source(environment, template)
        source = "\n".join(self.macros) + "\n" + source
        return source, filename, uptodate


class BaseAssetPackager:
    """Shared commands across asset packagers"""

    local_asset_path = None
    s3_asset_path = None

    def sync(self):
        """Sync the assets packaged"""
        if not self.local_asset_path:
            raise ValueError("missing local asset path for sync")
        if not self.s3_asset_path:
            raise ValueError("missing s3 asset path for sync")

        with subprocess.Popen(
            [
                "aws",
                "s3",
                "sync",
                self.local_asset_path,
                self.s3_asset_path,
                "--no-progress",
                "--acl",
                "bucket-owner-full-control",
            ],
            shell=False,
            stdout=subprocess.PIPE,
            universal_newlines=True,
        ) as p:
            for line in p.stdout:
                logger.info("s3 sync: %s" % line.strip())
        if p.returncode != 0:
            raise subprocess.CalledProcessError(p.returncode, p.args)


class RegionalAssetPackager(BaseAssetPackager):
    """Used to package regional assets"""

    def __init__(self, build_env: BuildEnvironment, region="us-east-1"):
        self.build_env = build_env
        self.local_asset_path = build_env.build_dist_dir
        self.s3_asset_path = f"s3://{build_env.source_bucket_name}-{region}/{build_env.solution_name}/{build_env.version_code}"

    def package(self):
        logger.info("packaging regional assets")
        logger.info("copying lambdas to build directory")
        shutil.copytree(
            src=os.path.join(self.build_env.source_dir, "lambdas"),
            dst=self.build_env.build_dir,
            dirs_exist_ok=True,
        )

        lambda_dirs = [
            os.path.join(self.build_env.build_dir, dir)
            for dir in os.listdir(self.build_env.build_dir)
            if os.path.isdir(os.path.join(self.build_env.build_dir, dir))
        ]
        for lambda_dir in lambda_dirs:
            lambda_name = os.path.basename(lambda_dir)
            is_layer = lambda_name.startswith("lambda_")
            requirements_path = os.path.join(
                self.build_env.build_dir, lambda_name, "requirements.txt"
            )
            lambda_path = os.path.join(self.build_env.build_dir, lambda_name)

            if is_layer:
                install_path = os.path.join(
                    lambda_path, "python", "lib", "python3.8", "site-packages"
                )
                logger.info(
                    "%s is a lambda layer - will install to python 3.8 module path"
                    % lambda_name
                )
                os.makedirs(install_path)
            else:
                install_path = lambda_path
                logger.info("%s is a lambda function - will install directly" % lambda_name)
                logger.info("%s copying shared libraries" % lambda_name)
                shutil.copytree(
                    src=os.path.join(self.build_env.source_dir, "shared"),
                    dst=os.path.join(lambda_dir, "shared"),
                    dirs_exist_ok=True,
                )

            if os.path.exists(requirements_path) and os.path.isfile(requirements_path):
                logger.info("%s installing Python requirements" % lambda_name)

                with subprocess.Popen(
                    [
                        "pip",
                        "install",
                        "-t",
                        install_path,
                        "-r",
                        requirements_path,
                        "--no-color",
                    ],
                    shell=False,
                    stdout=subprocess.PIPE,
                    universal_newlines=True,
                ) as p:
                    for line in p.stdout:
                        logger.info("%s pip: %s" % (lambda_name, line.strip()))
                if p.returncode != 0:
                    raise subprocess.CalledProcessError(p.returncode, p.args)
            else:
                logger.debug("%s has no Python requirements" % lambda_name)

            logger.info("%s packaging into .zip file" % lambda_name)
            archive = shutil.make_archive(
                base_name=lambda_path, format="zip", root_dir=lambda_path, logger=logger
            )
            shutil.move(archive, self.build_env.build_dist_dir)

        logger.info("packaging Jupyter Notebooks")
        shutil.copytree(
            src=self.build_env.notebook_dir,
            dst=os.path.join(self.build_env.build_dist_dir, "notebooks"),
        )


class GlobalAssetPackager(BaseAssetPackager):
    """Used to package global assets"""

    def __init__(self, build_env: BuildEnvironment):
        self.build_env = build_env
        self.local_asset_path = build_env.template_dist_dir
        self.s3_asset_path = f"s3://{build_env.source_bucket_name}/{build_env.solution_name}/{build_env.version_code}"

    def package(self):
        logger.info("packaging global assets")
        logger.info(
            "packaging CloudFormation template for %s:%s"
            % (self.build_env.solution_name, self.build_env.version_code)
        )
        shutil.copyfile(
            src=os.path.join(
                self.build_env.template_dir, f"{self.build_env.solution_name}.yaml"
            ),
            dst=os.path.join(
                self.build_env.template_dist_dir,
                f"{self.build_env.solution_name}.template",
            ),
        )

        # allow templates to be loaded from template folder or source
        template_loader = FileSystemLoader(self.build_env.template_dist_dir)
        source_loader = FileSystemLoader(self.build_env.source_dir)
        choice_loader = ChoiceLoader([template_loader, source_loader])

        # append these macros to the template at render time
        macro = (
            "{%- macro substitute(filename) %}" "{%include filename%}" "{% endmacro -%}"
        )
        macro_loader = GlobalMacroLoader(choice_loader)
        macro_loader.add_macro(macro)

        # set up the environment to render the template
        j2env = Environment(
            loader=macro_loader, variable_start_string="%%", variable_end_string="%%"
        )
        template = j2env.get_template(f"{self.build_env.solution_name}.template")
        rendered = template.render(
            VERSION=self.build_env.version_code,
            BUCKET_NAME=self.build_env.source_bucket_name,
            SOLUTION_NAME=self.build_env.solution_name,
            NOTEBOOKS='",'.join(os.listdir(self.build_env.notebook_dir)),
            DEPLOYMENT_ID=self.build_env.deployment_id,
        )

        # output the rendered template
        with open(
            os.path.join(
                self.build_env.template_dist_dir,
                f"{self.build_env.solution_name}.template",
            ),
            "w",
        ) as f:
            f.write(rendered)


@click.command()
@click.option(
    "--source-bucket-name",
    help="Name for the S3 bucket location where the template will source the Lambda.",
    required=True,
)
@click.option("--solution-name", help="The name of the solution.", required=True)
@click.option("--version-code", help="The version of the package.", required=True)
@click.option(
    "--log-level",
    help="The log level to use",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
@click.option(
    "--dev",
    help="Use this to add development-specific options to the template build",
    default=False,
    is_flag=True,
)
@click.option(
    "--sync",
    help="Use this to sync your assets to the global and regional source-buckets.",
    default=False,
    is_flag=True,
)
def package_assets(
    source_bucket_name, solution_name, version_code, log_level, dev, sync
):
    """Builds the global and regional S3 assets for this project"""

    logger.setLevel(log_level)

    # Set up relevant directories and clean the build environment
    build_env = BuildEnvironment(
        source_bucket_name=source_bucket_name,
        solution_name=solution_name,
        version_code=version_code,
        dev_mode=dev,
    )
    build_env.clean()

    # run global asset packaging
    gap = GlobalAssetPackager(build_env)
    gap.package()

    # run regional asset packaging
    rap = RegionalAssetPackager(build_env)
    rap.package()

    # sync the assets as required
    if sync:
        gap.sync()
        rap.sync()


if __name__ == "__main__":
    package_assets()
