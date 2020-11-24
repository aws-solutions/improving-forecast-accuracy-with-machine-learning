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
from functools import wraps
from pathlib import Path

import pytest
from aws_cdk import core

from interfaces import SolutionStackSubstitions
from stack import ForecastStack

logger = logging.getLogger("cdk-helper")


def requires_tests(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        logging.info("running infrastructure tests")
        pytest.main()
        logging.info("infrastructure tests complete.")
        return f(*args, **kwargs)

    return wrapper


def requires_context(required_context=None):
    context = required_context if required_context else []

    def get_context():
        try:
            with open("cdk.json", "r") as f:
                config = json.loads(f.read())
        except FileNotFoundError:
            return {}
        context = config.get("context", {})
        return context

    # get the context from the default cdk.json file
    context = get_context()

    # override/ set any required context from environment variables
    for var in required_context:
        value = os.environ.get(var)
        if value:
            context[var] = value
        elif not var and not context.get(var):
            raise ValueError(f"Missing CDK context or environment variable for {var}")

    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            # override/ set any required context from method parameters
            context.update({k.upper(): v for (k, v) in kwargs.items()})
            return f(context, *args, **kwargs)

        return wrapper

    return decorator


def notebooks_context():
    """Prepare the NOTEBOOKS CDK context variable for this app"""
    notebooks = (
        Path(__file__)
        .absolute()
        .parent.parent.parent.joinpath("notebook", "samples", "notebooks")
        .glob("*.ipynb")
    )
    return {"NOTEBOOKS": '","'.join([notebook.name for notebook in notebooks])}


# @requires_tests
@requires_context(["BUCKET_NAME", "SOLUTION_NAME", "VERSION", "QUICKSIGHT_SOURCE"])
def cdk(context, **kwargs):
    logger.info("running CDK")
    context.update(notebooks_context())
    app = core.App(
        runtime_info=False,
        stack_traces=False,
        tree_metadata=False,
        analytics_reporting=False,
        context=context,
    )
    synthesizer = SolutionStackSubstitions(qualifier="hnb659fds")
    stack = ForecastStack(app, "forecast-stack-cdk", synthesizer=synthesizer)
    return app.synth(force=True)
