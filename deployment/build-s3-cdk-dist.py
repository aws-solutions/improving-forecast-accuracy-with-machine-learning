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
from dataclasses import dataclass, field

import click

from infrastructure import cdk

logger = logging.getLogger("cdk-helper")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler()
formatter = logging.Formatter("[%(levelname)s]\t%(name)s\t%(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


class Cleaner:
    """Encapsulates functions that help clean up the build environment."""

    TO_CLEAN = [
        {"name": "Python bytecode", "file_type": "f", "pattern": "*.py[cod]"},
        {"name": "Python Coverage databases", "file_type": "f", "pattern": ".coverage"},
        {"name": "CDK Cloud Assemblies", "file_type": "d", "pattern": "cdk.out"},
        {"name": "Python eggs", "file_type": "d", "pattern": "*.egg-info"},
        {
            "name": "Python bytecode cache directory",
            "file_type": "d",
            "pattern": "__pycache__",
        },
        {"name": "Python test cache", "pattern": ".pytest_cache", "file_type": "d",},
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

            if file_type == "d":
                deletion_command = ["-exec", "rm", "-rf", "{}", "+"]
            else:
                deletion_command = ["-delete"]

            command.extend(deletion_command)

            logger.info("cleaning up %s" % name)
            result = subprocess.run(command)
            if result.returncode != 0:
                logging.error("Failed to clean up" % name)
        os.chdir(pwd)


@dataclass
class BuildEnvironment:
    source_bucket_name: str
    solution_name: str
    version_code: str
    template_dir: str = field(default_factory=os.getcwd, init=False)
    template_dist_dir: str = field(init=False, repr=False)
    build_dir: str = field(init=False, repr=False)
    build_dist_dir: str = field(init=False, repr=False)
    source_dir: str = field(init=False, repr=False)
    notebook_dir: str = field(init=False, repr=False)
    infrastructure_dir: str = field(init=False, repr=False)

    def __post_init__(self):
        self.template_dist_dir = os.path.join(self.template_dir, "global-s3-assets")
        self.build_dir = os.path.join(self.template_dir, "build-s3-assets")
        self.build_dist_dir = os.path.join(self.template_dir, "regional-s3-assets")
        self.source_dir = os.path.normpath(
            os.path.join(self.template_dir, os.pardir, "source")
        )
        self.notebook_dir = os.path.join(
            self.source_dir, "notebook", "samples", "notebooks"
        )
        self.infrastructure_dir = os.path.join(self.source_dir, "infrastructure")

        logger.debug("build environment template directory: %s" % self.template_dir)
        logger.debug(
            "build environment template distribution directory: %s"
            % self.template_dist_dir
        )
        logger.debug("build environment build directory: %s" % self.build_dir)
        logger.debug(
            "build environment build distribution directory: %s" % self.build_dist_dir
        )
        logger.debug("build environment source directory: %s" % self.source_dir)
        logger.debug("build environment notebook directory: %s" % self.notebook_dir)
        logger.debug(
            "build environment infrastructure directory: %s" % self.infrastructure_dir
        )

    def clean(self):
        """Clean up the build environment"""
        cleaner = Cleaner()
        cleaner.clean_dirs(self.template_dist_dir, self.build_dir, self.build_dist_dir)
        cleaner.cleanup_source(self.source_dir)


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

        logger.info("packaging Jupyter Notebooks")
        shutil.copytree(
            src=self.build_env.notebook_dir,
            dst=os.path.join(self.build_env.build_dist_dir, "notebooks"),
        )


@click.command()
@click.option(
    "--source-bucket-name",
    help="Name for the S3 bucket location where the template will source the Lambda.",
    required=True,
)
@click.option("--solution-name", help="The name of the solution.", required=True)
@click.option("--version-code", help="The version of the package.", required=True)
@click.option(
    "--dist-account-id",
    help="The AWS account id from which the Amazon QuickSight templates should be sourced for Amazon QuickSight Analysis and Dashboard creation.",
    required=True,
)
@click.option(
    "--dist-quicksight-namespace",
    help="The namespace (template prefix) to use together with --dist-account-id from which the Amazon QuickSight template should be sourced for Amazon QuickSight Analysis and Dashboard creation.",
    required=True,
)
@click.option(
    "--log-level",
    help="The log level to use",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
@click.option(
    "--sync",
    help="Use this to sync your assets to the global and regional source-buckets.",
    default=False,
    is_flag=True,
)
def package_assets(
    source_bucket_name,
    solution_name,
    version_code,
    dist_account_id,
    dist_quicksight_namespace,
    log_level,
    sync,
):
    """Runs the CDK build of the project, uploading assets as required."""

    logger.setLevel(log_level)

    # set up relevant directories and clean the build environment
    build_env = BuildEnvironment(
        source_bucket_name=source_bucket_name,
        solution_name=solution_name,
        version_code=version_code,
    )
    build_env.clean()

    # create quicksight source template ID
    quicksight_template_name = "_".join(
        [dist_quicksight_namespace, solution_name, version_code.replace(".", "_")]
    )
    quicksight_source = ":".join(
        [
            "arn:aws:quicksight:us-east-1",
            dist_account_id,
            f"template/{quicksight_template_name}",
        ]
    )

    # run cdk asset packaging
    cdk(
        bucket_name=source_bucket_name,
        solution_name=solution_name,
        version=version_code,
        quicksight_source=quicksight_source,
        solutions_assets_regional=build_env.build_dist_dir,
        solutions_assets_global=build_env.template_dist_dir,
    )

    # run regional asset packaging
    rap = RegionalAssetPackager(build_env)
    rap.package()

    if sync:
        rap.sync()


if __name__ == "__main__":
    package_assets()
