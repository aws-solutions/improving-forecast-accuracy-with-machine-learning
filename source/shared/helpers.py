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

from functools import wraps
from os import environ

import boto3
from botocore.stub import Stubber

from shared.logging import get_logger

logger = get_logger(__name__)

# declaring these global makes initialization/ performance a bit better if generating many forecasts
helpers_sts_client = None
helpers_forecast_client = None
helpers_sns_client = None
helpers_s3_client = None
helpers_sfn_client = None


class ResourcePending(Exception):
    pass


class ResourceFailed(Exception):
    pass


class ResourceInvalid(Exception):
    pass


class EnvironmentVariableError(Exception):
    pass


def step_function_step(f):
    """
    Used to wrap AWS Lambda Functions that produce an AWS Forecast resource status.
    :param f: the function to wrap
    :return: the wrapped function
    """

    @wraps(f)
    def wrapper(event, context):
        (status, output) = f(event, context)

        if status.failed:
            raise ResourceFailed
        elif status.updating:
            raise ResourcePending
        elif status.finalized:
            return output
        else:
            raise ResourceInvalid(f"This should not happen: Status is {status}")

    return wrapper


def get_aws_region():
    """
    Get the caller's AWS region from the environment variable AWS_REGION
    :return: the AWS region name (e.g. us-east-1)
    """
    region = environ.get("AWS_REGION")
    if not region:
        raise EnvironmentVariableError("Missing AWS_REGION environment variable.")

    return region


def get_account_id():
    """
    Get the caller's AWS account ID
    :return: The AWS account ID
    """
    global helpers_sts_client
    if not helpers_sts_client:
        region = get_aws_region()
        logger.debug("Initializing boto3 client for sts in %s" % region)
        helpers_sts_client = boto3.client("sts", region_name=get_aws_region())

    return helpers_sts_client.get_caller_identity().get("Account")


def get_forecast_client():
    """Get the global forecast boto3 client"""
    global helpers_forecast_client
    if not helpers_forecast_client:
        region = get_aws_region()
        logger.debug("Initializing boto3 client for forecast in %s" % region)
        helpers_forecast_client = boto3.client("forecast", region_name=get_aws_region())

    return helpers_forecast_client


def get_sns_client():
    """Get the global sns boto3 client"""
    global helpers_sns_client
    if not helpers_sns_client:
        region = get_aws_region()
        logger.debug("Initializing boto3 client for sns in %s" % region)
        helpers_sns_client = boto3.client("sns", region_name=get_aws_region())

    return helpers_sns_client


def get_s3_client():
    """Get the global s3 boto3 client"""
    global helpers_s3_client
    if not helpers_s3_client:
        region = get_aws_region()
        logger.debug("Initializing boto3 client for s3 in %s" % region)
        helpers_s3_client = boto3.client("s3", region_name=get_aws_region())

    return helpers_s3_client


def get_sfn_client():
    """Get the global step functions boto3 client"""
    global helpers_sfn_client
    if not helpers_sfn_client:
        region = get_aws_region()
        logger.debug("Initializing boto3 client for stepfunctions in %s" % region)
        helpers_sfn_client = boto3.client("stepfunctions", region_name=get_aws_region())

    return helpers_sfn_client


class InputValidator:
    def __init__(self, method, **expected_params):
        self.method = method
        self.expected_params = expected_params

    def validate(self):
        """
        Validate an Amazon Forecast resource using the botocore stubber
        :return: None. Raises ParamValidationError if the InputValidator fails to validate
        """
        cli = get_forecast_client()
        func = getattr(cli, self.method)
        with Stubber(cli) as stubber:
            stubber.add_response(self.method, {}, self.expected_params)
            func(**self.expected_params)


def camel_to_snake(s):
    """
    Convert a camelCasedName to a snake_cased_name
    :param s: the camelCasedName
    :return: the snake_cased_name
    """
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


class ForecastClient:
    """Validate a resource from Amazon Forecast against its resource model"""

    def __init__(self, resource, **resource_creation_kwargs):
        self.account_id = get_account_id()
        self.region = get_aws_region()
        self.cli = get_forecast_client()
        self.resource = resource
        self.validator = InputValidator(
            f"create_{self.resource}", **resource_creation_kwargs
        )

    @classmethod
    def validate_config(cls, **resource_creation_kwargs):
        """
        Validate the configuration of a resource by name and arguments. This allows for validation of resources before
        they are fully initialized.
        :param resource_creation_kwargs: the arguments to pass to the service
        :return: None. Raises ParamValidationError if the InputValidator fails to validate
        """
        method_name = f"create_{camel_to_snake(cls.__name__)}"
        InputValidator(method_name, **resource_creation_kwargs).validate()

    def validate(self):
        """
        Validate an instance of a resource
        :return: None. Raises ParamValidationError if the InputValidator fails to validate
        """
        self.validator.validate()
