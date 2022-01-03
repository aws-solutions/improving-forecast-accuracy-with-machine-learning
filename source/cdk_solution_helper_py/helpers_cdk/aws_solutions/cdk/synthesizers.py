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
import json
import logging
import os
import re
import shutil
from contextlib import suppress
from dataclasses import field, dataclass
from fileinput import FileInput
from pathlib import Path
from typing import List, Dict

import jsii
from aws_cdk.core import IStackSynthesizer, DefaultStackSynthesizer, ISynthesisSession

logger = logging.getLogger("cdk-helper")


@dataclass
class CloudFormationTemplate:
    """Encapsulates the transformations that are required on a CDK generated CloudFormation template for AWS Solutions"""

    path: Path
    contents: Dict = field(repr=False)
    assets: Path = field(repr=False)
    stack_name: str = field(repr=False, init=False)
    cloud_assembly_path: Path = field(repr=False, init=False)
    assets_global: List[Path] = field(repr=False, default_factory=list, init=False)
    assets_regional: List[Path] = field(repr=False, default_factory=list, init=False)
    global_asset_name: str = field(repr=False, init=False)

    def __post_init__(self):
        self.cloud_assembly_path = self.path.parent
        self.stack_name = self.path.stem.split(".")[0]
        self.assets_global.append(self.path)
        try:
            self.global_asset_name = self.contents["Metadata"][
                "aws:solutions:templatename"
            ]
        except KeyError:
            logger.warning(
                "for nested stack support, you must provide a filename to TemplateOptions for each stack"
            )

    def delete_bootstrap_parameters(self):
        """Remove the CDK bootstrap parameters, since this stack will not be bootstrapped"""
        with suppress(KeyError):
            del self.contents["Parameters"]["BootstrapVersion"]

        with suppress(KeyError):
            if len(self.contents["Parameters"]) == 0:
                del self.contents["Parameters"]

        with suppress(KeyError):
            del self.contents["Rules"]["CheckBootstrapVersion"]

        with suppress(KeyError):
            if len(self.contents["Rules"]) == 0:
                del self.contents["Rules"]

    def delete_cdk_helpers(self):
        """Remove the CDK bucket deployment helpers, since solutions don't have a bootstrap bucket."""
        to_delete = []
        for (resource_name, resource) in self.contents.get("Resources", {}).items():
            if "Custom::CDKBucketDeployment" in resource["Type"]:
                to_delete.append(resource_name)
            if "CDKBucketDeployment" in resource_name:
                to_delete.append(resource_name)
        for resource in to_delete:
            logger.info(f"deleting resource {resource}")
            del self.contents["Resources"][resource]

    def patch_nested(self):
        """Patch nested stacks for S3 deployment compatibility"""
        template_output_bucket = os.getenv(
            "TEMPLATE_OUTPUT_BUCKET",
            {
                "Fn::FindInMap": [  # NOSONAR (python:S1192) - string for clarity
                    "SourceCode",
                    "General",
                    "S3Bucket",
                ]
            },
        )
        for (resource_name, resource) in self.contents.get("Resources", {}).items():
            resource_type = resource.get("Type")
            if resource_type == "AWS::CloudFormation::Stack":
                try:
                    nested_stack_filename = resource["Metadata"][
                        "aws:solutions:templatename"
                    ]
                except KeyError:
                    raise KeyError("nested stack missing required TemplateOptions")

                # update CloudFormation resource properties for S3Bucket and S3Key
                # fmt: off
                resource["Properties"]["TemplateURL"] = {
                    "Fn::Join": [  # NOSONAR (python:S1192) - string for clarity
                        "",
                        [
                            "https://",
                            template_output_bucket,
                            ".s3.",
                            {"Ref": "AWS::URLSuffix"},
                            "/",
                            {
                                "Fn::FindInMap": ["SourceCode", "General", "KeyPrefix"]  # NOSONAR (python:S1192) - string for clarity
                            },
                            "/",
                            nested_stack_filename,
                        ],
                    ]
                }
                # fmt: on

    def patch_lambda(self):
        """Patch the lambda functions for S3 deployment compatibility"""
        for (resource_name, resource) in self.contents.get("Resources", {}).items():
            resource_type = resource.get("Type")
            if (
                resource_type == "AWS::Lambda::Function"
                or resource_type == "AWS::Lambda::LayerVersion"
            ):
                logger.info(f"{resource_name} ({resource_type}) patching")

                # the key for S3Key for AWS::Lambda:LayerVersion is under "Content".
                # the key for S3Key FOR AWS::Lambda::Function is under "Code"
                content_key = (
                    "Content"
                    if resource_type == "AWS::Lambda::LayerVersion"
                    else "Code"
                )
                try:
                    resource_id = resource["Properties"][content_key]["S3Key"].split(
                        "."
                    )[0]
                except KeyError:
                    logger.warning(
                        "found resource without an S3Key (this typically occurs when using inline code or during test)"
                    )
                    continue

                asset = self.assets["files"][resource_id]
                asset_source_path = self.path.parent.joinpath(asset["source"]["path"])
                asset_packaging = asset["source"]["packaging"]

                # CDK does not zip assets prior to deployment - we do it here if a zip asset is detected
                if asset_packaging == "zip":
                    # create archive if necessary
                    logger.info(f"{resource_name} packaging into .zip file")
                    archive = shutil.make_archive(
                        base_name=asset_source_path,
                        format="zip",
                        root_dir=str(asset_source_path),
                    )
                elif asset_packaging == "file":
                    archive = self.cloud_assembly_path.joinpath(asset["source"]["path"])
                else:
                    raise ValueError(
                        f"Unsupported asset packaging format: {asset_packaging}"
                    )

                # rename archive to match the resource name it was generated for
                archive_name = f"{resource_name}.zip"
                archive_path = self.cloud_assembly_path.joinpath(archive_name)
                shutil.move(src=archive, dst=archive_path)

                # update CloudFormation resource properties for S3Bucket and S3Key
                # fmt: off
                resource["Properties"][content_key]["S3Bucket"] = {
                    "Fn::Join": [ # NOSONAR (python:S1192) - string for clarity
                        "-",
                        [
                            {
                                "Fn::FindInMap": ["SourceCode", "General", "S3Bucket"]  # NOSONAR (python:S1192) - string for clarity
                            },
                            {"Ref": "AWS::Region"},
                        ],
                    ]
                }
                resource["Properties"][content_key]["S3Key"] = {
                    "Fn::Join": [  # NOSONAR (python:S1192) - string for clarity
                        "/",
                        [
                            {
                                "Fn::FindInMap": ["SourceCode", "General", "KeyPrefix"]  # NOSONAR (python:S1192) - string for clarity
                            },
                            archive_name,
                        ],
                    ]
                }
                # fmt: on

                # add resource to the list of regional assets
                self.assets_regional.append(archive_path)

    def _build_asset_path(self, asset_path):
        asset_output_path = self.cloud_assembly_path.joinpath(asset_path)
        asset_output_path.mkdir(parents=True, exist_ok=True)
        return asset_output_path

    def save(self, asset_path_global: Path = None, asset_path_regional: Path = None):
        """Save the template (will save to the asset paths if specified)"""
        self.path.write_text(json.dumps(self.contents, indent=2))

        # global solutions assets - default folder location is "global-s3-assets"
        if asset_path_global:
            asset_path = self._build_asset_path(asset_path_global)
            for asset in self.assets_global:
                shutil.copy(
                    str(asset),
                    str(asset_path.joinpath(self.global_asset_name)),
                )

        # regional solutions assets - default folder location is "regional-s3-assets"
        if asset_path_regional:
            asset_path = self._build_asset_path(asset_path_regional)
            for asset in self.assets_regional:
                shutil.copy(str(asset), str(asset_path))


@jsii.implements(IStackSynthesizer)
class SolutionStackSubstitions(DefaultStackSynthesizer):
    """Used to handle AWS Solutions template substitutions and sanitization"""

    substitutions = None
    substitution_re = re.compile("%%[a-zA-Z-_][a-zA-Z-_]+%%")

    def _template_names(self, session: ISynthesisSession) -> List[Path]:
        assembly_output_path = Path(session.assembly.outdir)
        templates = [assembly_output_path.joinpath(self._stack.template_file)]

        # add this stack's children to the outputs to process (todo: this only works for singly-nested stacks)
        for child in self._stack.node.children:
            child_template = getattr(child, "template_file", None)
            if child_template:
                templates.append(assembly_output_path.joinpath(child_template))
        return templates

    def _templates(self, session: ISynthesisSession) -> (Path, Dict):
        assembly_output_path = Path(session.assembly.outdir)

        assets = {}
        try:
            assets = json.loads(
                next(
                    assembly_output_path.glob(self._stack.stack_name + "*.assets.json")
                ).read_text()
            )
        except StopIteration:
            pass  # use the default (no assets)

        for path in self._template_names(session):
            yield CloudFormationTemplate(path, json.loads(path.read_text()), assets)

    def synthesize(self, session: ISynthesisSession):
        # when called with `cdk deploy` this outputs to cdk.out
        # when called from python directly, this outputs to a temporary directory
        result = DefaultStackSynthesizer.synthesize(self, session)

        asset_path_regional = self._stack.node.try_get_context(
            "SOLUTIONS_ASSETS_REGIONAL"
        )
        asset_path_global = self._stack.node.try_get_context("SOLUTIONS_ASSETS_GLOBAL")

        logger.info(
            f"solutions parameter substitution in {session.assembly.outdir} started"
        )
        for template in self._template_names(session):
            logger.info(f"substutiting parameters in {str(template)}")
            with FileInput(template, inplace=True) as template_lines:
                for line in template_lines:
                    # handle all template subsitutions in the line
                    for match in SolutionStackSubstitions.substitution_re.findall(line):
                        placeholder = match.replace("%", "")
                        replacement = self._stack.node.try_get_context(placeholder)
                        if not replacement:
                            raise ValueError(
                                f"Please provide a parameter substitution for {placeholder} via environment variable or CDK context"
                            )

                        line = line.replace(match, replacement)
                    # print the (now substituted) line in the context of template_lines
                    print(line, end="")
            logger.info(f"substituting parameters in {str(template)} completed")
        logger.info("solutions parameter substitution completed")

        # do not perform solution resource/ template cleanup if asset paths not passed
        if not asset_path_global or not asset_path_regional:
            return

        logger.info(
            f"solutions template customization in {session.assembly.outdir} started"
        )
        for template in self._templates(session):
            template.patch_lambda()
            template.patch_nested()
            template.delete_bootstrap_parameters()
            template.delete_cdk_helpers()
            template.save(
                asset_path_global=asset_path_global,
                asset_path_regional=asset_path_regional,
            )
        logger.info("solutions template customization completed")

        return result
