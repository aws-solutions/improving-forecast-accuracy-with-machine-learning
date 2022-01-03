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
from typing import Union

from aws_cdk.core import (
    Construct,
    CfnResource,
    Duration,
    Stack,
    CfnParameter,
    Aspects,
    Fn,
    CfnCondition,
)

from aws_solutions.cdk.aspects import ConditionalResources
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction


class UrlHelper(Construct):
    def __init__(self, scope: Construct, id: str, url: Union[str, CfnParameter]):
        super().__init__(scope, id)

        self.url_for = id
        self.url_provided = CfnCondition(
            self,
            f"{id}URLProvided",
            expression=Fn.condition_not(Fn.condition_equals(url, "")),
        )

        if isinstance(url, CfnParameter):
            url = url.value_as_string

        # name the function
        self.name = f"{id}UrlInfo".replace("_", "")

        # functions and permissions
        helper_function = self.url_helper_function()

        # Custom resource to perform the url inspection
        self.helper = CfnResource(
            self,
            self.name,
            type="Custom::UrlHelper",
            properties={
                "ServiceToken": helper_function.function_arn,
                "Url": url,
            },
        )
        Aspects.of(self.helper).add(ConditionalResources(self.url_provided))

    @property
    def properties(self):
        return {
            f"{self.url_for}Url": Fn.condition_if(
                self.url_provided.logical_id, self.helper.get_att("Url").to_string(), ""
            ).to_string(),
            f"{self.url_for}Scheme": Fn.condition_if(
                self.url_provided.logical_id,
                self.helper.get_att("Scheme").to_string(),
                "",
            ).to_string(),
            f"{self.url_for}Bucket": Fn.condition_if(
                self.url_provided.logical_id,
                self.helper.get_att("Bucket").to_string(),
                "",
            ).to_string(),
            f"{self.url_for}Key": Fn.condition_if(
                self.url_provided.logical_id,
                self.helper.get_att("Key").to_string(),
                "",
            ).to_string(),
        }

    def url_helper_function(self):
        stack = Stack.of(self)
        construct_id = "UrlHelper-24E40850-EBEF-42CD-A097-5895A851DC9F"
        exists = stack.node.try_find_child(construct_id)
        if exists:
            return exists
        else:
            return SolutionsPythonFunction(
                self,
                construct_id,
                entrypoint=Path(__file__).parent
                / "src"
                / "custom_resources"
                / "url_helper.py",
                function="handler",
                timeout=Duration.seconds(30),
            )
