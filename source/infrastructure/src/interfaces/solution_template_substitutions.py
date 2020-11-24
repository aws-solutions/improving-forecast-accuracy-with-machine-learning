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
import json
import logging
import os
import re
import shutil
from contextlib import suppress
from dataclasses import dataclass, field
from fileinput import FileInput
from pathlib import Path
from typing import Dict, List

import jsii
from aws_cdk.core import (
    IStackSynthesizer,
    ISynthesisSession,
    DefaultStackSynthesizer,
    App,
)

logger = logging.getLogger("cdk-helper")


@dataclass
class CloudFormationTemplate:
    path: Path
    contents: Dict = field(repr=False)
    stack_name: str = field(repr=False, init=False)
    cloud_assembly_path: Path = field(repr=False, init=False)
    assets: Dict = field(repr=False, init=False)
    assets_global: List[Path] = field(repr=False, default_factory=list, init=False)
    assets_regional: List[Path] = field(repr=False, default_factory=list, init=False)

    def __post_init__(self):
        self.cloud_assembly_path = self.path.parent
        self.stack_name = self.path.stem.split(".")[0]
        stack_assets_path = next(
            self.cloud_assembly_path.glob(self.stack_name + ".assets.json")
        )
        self.assets = json.loads(stack_assets_path.read_text())
        self.assets_global.append(self.path)

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
        to_delete = []
        for (resource_name, resource) in self.contents.get("Resources").items():
            if "Custom::CDKBucketDeployment" in resource["Type"]:
                to_delete.append(resource_name)
            if "CDKBucketDeployment" in resource_name:
                to_delete.append(resource_name)
        for resource in to_delete:
            logger.info(f"deleting resource {resource}")
            del self.contents["Resources"][resource]

    def patch_lambda(self):
        """Patch the lambda functions for S3 deployment compatibility"""
        for (resource_name, resource) in self.contents.get("Resources").items():
            resource_type = resource.get("Type")
            if (
                resource_type == "AWS::Lambda::Function"
                or resource_type == "AWS::Lambda::LayerVersion"
            ):
                logger.info(f"{resource_name} ({resource_type}) patching")

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
                        "found resource without an S3Key - skipping packaging"
                    )
                    continue

                asset = self.assets["files"][resource_id]
                asset_source_path = self.path.parent.joinpath(asset["source"]["path"])
                asset_packaging = asset["source"]["packaging"]

                if asset_packaging == "zip":
                    # create archive if necessary
                    logger.info(f"{resource_name} packaging into .zip file")
                    archive = shutil.make_archive(
                        base_name=asset_source_path,
                        format="zip",
                        root_dir=asset_source_path,
                    )
                elif asset_packaging == "file":
                    archive = self.cloud_assembly_path.joinpath(asset["source"]["path"])
                else:
                    raise ValueError(
                        f"Unsupported asset packaging format: {asset_packaging}"
                    )

                # rename archive
                archive_name = f"{resource_name}.zip"
                archive_path = self.cloud_assembly_path.joinpath(archive_name)
                shutil.move(src=archive, dst=archive_path)

                # update CloudFormation resource
                resource["Properties"][content_key]["S3Bucket"] = {
                    "Fn::Join": [
                        "-",
                        [
                            {"Fn::FindInMap": ["SourceCode", "General", "S3Bucket"]},
                            {"Ref": "AWS::Region"},
                        ],
                    ]
                }
                resource["Properties"][content_key]["S3Key"] = {
                    "Fn::Join": [
                        "/",
                        [
                            {"Fn::FindInMap": ["SourceCode", "General", "KeyPrefix"]},
                            archive_name,
                        ],
                    ]
                }

                # add resource to the list of regional assets
                self.assets_regional.append(archive_path)

    def save(self, asset_path_global: Path = None, asset_path_regional: Path = None):
        self.path.write_text(json.dumps(self.contents, indent=2))

        if asset_path_global:
            for asset in self.assets_global:
                shutil.copy(
                    str(asset),
                    Path(asset_path_global).joinpath(
                        "improving-forecast-accuracy-with-machine-learning.template"
                    ),
                )

        if asset_path_regional:
            for asset in self.assets_regional:
                shutil.copy(str(asset), str(asset_path_regional))


@jsii.implements(IStackSynthesizer)
class SolutionStackSubstitions(DefaultStackSynthesizer):
    substitutions = None
    substitution_re = re.compile("%%[a-zA-Z-_][a-zA-Z-_]+%%")

    @staticmethod
    def get_parameter(app: App, key: str):
        from_env = os.getenv(key, None)
        if from_env:
            return from_env

        from_ctx = app.node.try_get_context(key)
        if from_ctx:
            return from_ctx

        raise ValueError(f"Missing parameter: {key}")

    def _templates(self, session: ISynthesisSession) -> (Path, Dict):
        template_paths = Path(session.assembly.outdir).glob("*.template.json")
        for path in template_paths:
            yield CloudFormationTemplate(path, json.loads(path.read_text()))

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
        templates = Path(session.assembly.outdir).glob("*.template.json")
        for template in templates:
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
        logger.info("solutions parameter substitution completed")

        # do not perform solution resource/ template cleanup if asset paths not passed
        if not asset_path_global or not asset_path_regional:
            return

        logger.info(
            f"solutions template customization in {session.assembly.outdir} started"
        )
        for template in self._templates(session):
            template.patch_lambda()
            template.delete_bootstrap_parameters()
            template.delete_cdk_helpers()
            template.save(
                asset_path_global=asset_path_global,
                asset_path_regional=asset_path_regional,
            )
        logger.info("solutions template customization completed")

        return result
