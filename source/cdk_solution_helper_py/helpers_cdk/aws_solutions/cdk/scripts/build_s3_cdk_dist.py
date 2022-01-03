#!/usr/bin/env python3

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

import os
import re
import shutil
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import List

import boto3
import botocore
import click

from aws_solutions.cdk.helpers import copytree
from aws_solutions.cdk.helpers.loader import load_cdk_app
from aws_solutions.cdk.helpers.logger import Logger
from aws_solutions.cdk.tools import Cleaner

logger = Logger.get_logger("cdk-helper")


class PathPath(click.Path):
    def convert(self, value, param, ctx):
        return Path(super().convert(value, param, ctx))


@dataclass
class BuildEnvironment:
    source_bucket_name: str = field(default="")
    solution_name: str = field(default="")
    version_code: str = field(default="")
    extra_regional_assets: List[Path] = field(repr=False, default_factory=list)
    template_dir: str = field(default_factory=os.getcwd, init=False)
    template_dist_dir: str = field(init=False, repr=False)
    build_dir: str = field(init=False, repr=False)
    build_dist_dir: str = field(init=False, repr=False)
    source_dir: str = field(init=False, repr=False)
    infrastructure_dir: str = field(init=False, repr=False)

    def __post_init__(self):
        self.template_dist_dir = os.path.join(self.template_dir, "global-s3-assets")
        self.build_dir = os.path.join(self.template_dir, "build-s3-assets")
        self.build_dist_dir = os.path.join(self.template_dir, "regional-s3-assets")
        self.source_dir = os.path.normpath(
            os.path.join(self.template_dir, os.pardir, "source")
        )
        self.infrastructure_dir = os.path.join(self.source_dir, "infrastructure")
        self.open_source_dir = os.path.join(self.template_dir, "open-source")
        self.github_dir = os.path.normpath(
            os.path.join(self.template_dir, os.pardir, ".github")
        )

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
        logger.debug(
            "build environment infrastructure directory: %s" % self.infrastructure_dir
        )
        logger.debug("open source dir: %s" % self.open_source_dir)

    def clean_for_scan(self):
        """Clean up the build environment partially to optimize code scan in next build stage"""
        cleaner = Cleaner()
        cleaner.cleanup_source(self.source_dir)
        return cleaner

    def clean(self):
        """Clean up the build environment"""
        cleaner = self.clean_for_scan()
        cleaner.clean_dirs(self.template_dist_dir, self.build_dir, self.build_dist_dir)
        return cleaner

    def clean_for_open_source(self):
        """Clean up the build environment for the open source build"""
        cleaner = self.clean_for_scan()
        cleaner.clean_dirs(self.open_source_dir)
        return cleaner


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

        self.check_bucket()
        try:
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
                stderr=subprocess.PIPE,
                universal_newlines=True,
            ) as p:
                for line in p.stdout:
                    logger.info("s3 sync: %s" % line.strip())
                for line in p.stderr:
                    logger.error("s3 sync: %s" % line.strip())
        except FileNotFoundError:
            logger.error("awscli is missing")
            raise click.ClickException("--sync requires awscli to be installed")
        if p.returncode != 0:
            raise click.ClickException("--sync failed")

    def check_bucket(self) -> bool:
        """Checks bucket ownership before sync"""
        bucket = self.s3_asset_path.split("/")[2]
        sts = boto3.client("sts")
        account = sts.get_caller_identity()["Account"]

        s3 = boto3.client("s3")
        try:
            s3.head_bucket(Bucket=bucket, ExpectedBucketOwner=account)
        except botocore.exceptions.ClientError as err:
            status = err.response["ResponseMetadata"]["HTTPStatusCode"]
            error = err.response["Error"]["Code"]
            if status == 404:
                logger.error("missing bucket: %s" % error)
            elif status == 403:
                logger.error("access denied - check bucket ownership: %s" % error)
            else:
                logger.exception("unknown error: %s" % error)
            raise
        return True


class RegionalAssetPackager(BaseAssetPackager):
    """Used to package regional assets"""

    def __init__(self, build_env: BuildEnvironment, region="us-east-1"):
        self.build_env = build_env
        self.local_asset_path = build_env.build_dist_dir
        self.s3_asset_path = f"s3://{build_env.source_bucket_name}-{region}/{build_env.solution_name}/{build_env.version_code}"

    def package(self):
        logger.info("packaging regional assets")

        for asset in self.build_env.extra_regional_assets:
            if asset.is_dir():
                name = asset.name
                logger.info(f"packaging regional assets for {name}")
                shutil.copytree(
                    src=asset, dst=Path(self.build_env.build_dist_dir) / name
                )
            elif asset.is_file():
                raise click.ClickException(
                    "--extra-regional-assets must be paths to directories"
                )


class GlobalAssetPackager(BaseAssetPackager):
    """Used to package global assets"""

    def __init__(self, build_env: BuildEnvironment):
        self.build_env = build_env
        self.local_asset_path = build_env.template_dist_dir
        self.s3_asset_path = f"s3://{build_env.source_bucket_name}/{build_env.solution_name}/{build_env.version_code}"

    def package(self):
        logger.info("packaging global assets")


def validate_version_code(ctx, param, value):
    """
    Version codes are validated as semantic versions prefixed by a v, e.g. v1.2.3
    :param ctx: the click context
    :param param: the click parameter
    :param value: the parameter value
    :return: the validated value
    """
    re_semver = r"^v(?P<major>0|[1-9]\d*)\.(?P<minor>0|[1-9]\d*)\.(?P<patch>0|[1-9]\d*)(?:-(?P<prerelease>(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+(?P<buildmetadata>[0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$"
    if re.match(re_semver, value):
        return value
    else:
        raise click.BadParameter(
            "please specifiy major, minor and patch versions, e.g. v1.0.0"
        )


@click.group()
@click.option(
    "--log-level",
    help="The log level to use",
    default="INFO",
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]),
)
@click.pass_context
def cli(ctx, log_level):
    """This CLI helps to build and package your AWS Solution CDK application as CloudFormation templates"""
    ctx.ensure_object(dict)
    logger.setLevel(log_level)


@cli.command()
@click.pass_context
def clean_for_scan(ctx):
    """Use this to partially clean generated build files to optimize code scan in next build stage"""
    env = BuildEnvironment()
    env.clean_for_scan()


@cli.command()
@click.pass_context
@click.option("--ignore", "-i", multiple=True)
@click.option("--solution-name", help="The name of the solution.", required=True)
def source_code_package(ctx, ignore, solution_name):
    """Use this to build the source package folder and zip file"""
    env = BuildEnvironment()
    env.clean_for_open_source()

    # set up some default ignore directories
    ignored = [
        "**/cdk.out/*",
        "**/__pycache__/*",
        "*.pyc",
        "*.pyo",
        "*.pyd",
        "**/.gradle/*",
        "**/.idea/*",
        "**/.coverage/*",
        "**/.pytest_cache/*",
        "**/*.egg-info",
        "**/__pycache__",
    ]
    ignored.extend(ignore)

    required_files = [
        "LICENSE.txt",
        "NOTICE.txt",
        "README.md",
        "CODE_OF_CONDUCT.md",
        "CONTRIBUTING.md",
        "CHANGELOG.md",
        ".gitignore",
    ]

    # copy source directory
    try:
        copytree(
            env.source_dir, os.path.join(env.open_source_dir, "source"), ignore=ignored
        )
        copytree(env.github_dir, os.path.join(env.open_source_dir, ".github"))
    except FileNotFoundError:
        raise click.ClickException(
            "The solution requires a `source` folder and a `.github` folder"
        )

    # copy all required files
    for name in required_files:
        try:
            shutil.copyfile(
                Path(env.source_dir).parent / name, Path(env.open_source_dir) / name
            )
        except FileNotFoundError:
            raise click.ClickException(
                f"The solution is missing the required file {name}"
            )

    # copy the required run-unit-tests.sh
    (Path(env.open_source_dir) / "deployment").mkdir()
    try:
        shutil.copyfile(
            Path(env.template_dir) / "run-unit-tests.sh",
            Path(env.open_source_dir) / "deployment" / "run-unit-tests.sh",
        )
    except FileNotFoundError:
        raise click.ClickException(
            f"The solution is missing deployment/run-unit-tests.sh"
        )

    shutil.make_archive(
        base_name=os.path.join(env.template_dir, solution_name),
        format="zip",
        root_dir=os.path.join(env.open_source_dir),
        logger=logger,
    )

    # finalize by deleting the open-source folder data and copying the zip file over
    env.clean_for_open_source()
    shutil.move(
        os.path.join(env.template_dir, f"{solution_name}.zip"), env.open_source_dir
    )


@cli.command()
@click.pass_context
@click.option(
    "--source-bucket-name",
    help="Configure the bucket name of your target Amazon S3 distribution bucket. A randomized value is recommended. "
    "You will also need to create an S3 bucket where the name is <source-bucket-name>-<region>. The solution's "
    "CloudFormation template will expect the source code to be located in a bucket matching that name.",
    required=True,
)
@click.option("--solution-name", help="The name of the solution.", required=True)
@click.option(
    "--version-code",
    help="The version of the package.",
    required=True,
    callback=validate_version_code,
)
@click.option(
    "--cdk-app-path",
    help="The CDK Python app path",
    required=True,
    type=PathPath(dir_okay=False),
)
@click.option(
    "--cdk-app-entrypoint",
    help="The CDK Python app entrypoint",
    required=True,
)
@click.option(
    "--sync",
    help="Use this to sync your assets to the global and regional source-buckets.",
    default=False,
    is_flag=True,
)
@click.option(
    "--region",
    help="Use this flag to control which regional bucket to push your assets to",
    default="us-east-1",
)
@click.option(
    "--extra-regional-assets",
    help="Use this flag to package additional regional deployment assets",
    multiple=True,
    type=PathPath(dir_okay=True),
)
def deploy(
    ctx,  # NOSONAR (python:S107) - allow large number of method parameters
    source_bucket_name,
    solution_name,
    version_code,
    cdk_app_path,
    cdk_app_entrypoint,
    sync,
    region,
    extra_regional_assets,
):
    """Runs the CDK build of the project, uploading assets as required."""

    # load the cdk app dynamically
    cdk = load_cdk_app(
        cdk_app_path=cdk_app_path,
        cdk_app_name=cdk_app_entrypoint,
    )

    # set up relevant directories and clean the build environment
    env = BuildEnvironment(
        source_bucket_name=source_bucket_name,
        solution_name=solution_name,
        version_code=version_code,
        extra_regional_assets=extra_regional_assets,
    )

    # clean up the build environment from previous builds before running this build
    env.clean()

    # run cdk asset packaging
    cdk(
        {
            "BUCKET_NAME": source_bucket_name,
            "SOLUTION_NAME": solution_name,
            "SOLUTION_VERSION": version_code,
            "SOLUTIONS_ASSETS_REGIONAL": env.build_dist_dir,
            "SOLUTIONS_ASSETS_GLOBAL": env.template_dist_dir,
        }
    )

    # run regional asset packaging
    rap = RegionalAssetPackager(env, region=region)
    rap.package()

    # run global asset packaging
    gap = GlobalAssetPackager(env)
    gap.package()

    # sync as required
    if sync:
        rap.sync()
        gap.sync()


if __name__ == "__main__":
    cli()
