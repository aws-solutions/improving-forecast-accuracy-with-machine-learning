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
from functools import wraps
from os import environ
from pathlib import Path
from typing import Union

ARGUMENT_ERROR = "functions decorated with `with_cdk_context` can only accept one dictionary argument - the additional context overrides to use"

logger = logging.getLogger("cdk-helper")


class SolutionContext:
    def __init__(self, cdk_json_path: Union[None, Path] = None):
        self.cdk_json_path = cdk_json_path
        self.context = self._load_cdk_context()

    def requires(  # NOSONAR - higher cognitive complexity allowed
        self, context_var_name, context_var_value=None
    ):
        context = self.context

        def cdk_context_decorator(f):
            @wraps(f)
            def wrapper(*args):
                # validate function arguments
                if len(args) > 1:
                    raise ValueError(ARGUMENT_ERROR)
                if len(args) == 1 and not isinstance(args[0], dict):
                    raise TypeError(ARGUMENT_ERROR)

                if len(args) == 0:
                    args = (context,)

                # override the CDK context as required
                if len(args) == 1:
                    context.update(args[0])

                    env_context_var = environ.get(context_var_name)
                    if env_context_var:
                        context[context_var_name] = env_context_var
                    elif context_var_name and context_var_value:
                        context[context_var_name] = context_var_value

                    if not context.get(context_var_name):
                        raise ValueError(
                            f"Missing cdk.json context variable or environment variable for {context_var_name}."
                        )

                    args = (context,)

                return f(*args)

            return wrapper

        return cdk_context_decorator

    def _load_cdk_context(self):
        """Load context from cdk.json"""
        if not self.cdk_json_path:
            return {}

        try:
            with open(self.cdk_json_path, "r") as f:
                config = json.loads(f.read())
        except FileNotFoundError:
            logger.warning(f"{self.cdk_json_path} not found, using empty context!")
            return {}
        context = config.get("context", {})
        return context
