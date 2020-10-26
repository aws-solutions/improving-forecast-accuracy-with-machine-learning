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
from botocore.config import Config
from botocore.stub import Stubber

from shared.logging import get_logger

logger = get_logger(__name__)

SOLUTION_ID = "SOL0123"
CLIENT_CONFIG = Config(retries={"max_attempts": 10, "mode": "standard"})

# declaring these global makes initialization/ performance a bit better if generating many forecasts
_helpers_service_clients = dict()


class ResourcePending(Exception):
    pass


class ResourceFailed(Exception):
    pass


class ResourceInvalid(Exception):
    pass


class EnvironmentVariableError(Exception):
    pass


class DatasetsImporting(Exception):
    pass


def step_function_step(f):
    """
    Used to wrap AWS Lambda Functions that produce an AWS Forecast resource status.
    :param f: the function to wrap
    :return: the wrapped function
    """

    @wraps(f)
    def wrapper(event, context):
        try:
            (status, output) = f(event, context)
        except get_forecast_client().exceptions.ResourceInUseException:
            logger.info("resource is currently updating")
            raise ResourcePending
        except get_forecast_client().exceptions.LimitExceededException as limit_exception:  # deal with (retryable) forecast rate limiting
            if "concurrently" in str(limit_exception):
                raise ResourcePending
            elif "dataset import jobs" in str(
                limit_exception
            ):  # "Quota limit of \d+ dataset import jobs has been reached"
                raise ResourcePending
            else:
                raise limit_exception  # reraise - user will have to make changes to forecast or file a quota change request ticket
        else:
            if status.failed:
                raise ResourceFailed
            elif status.updating:
                raise ResourcePending
            elif status.finalized:
                return output
            else:
                raise ResourceInvalid(f"This should not happen: Status is {status}")

    return wrapper


def get_service_client(service_name, config=CLIENT_CONFIG):
    global _helpers_service_clients
    if service_name not in _helpers_service_clients:
        logger.debug(f"Initializing global boto3 client for {service_name}")
        _helpers_service_clients[service_name] = boto3.client(
            service_name, config=config, region_name=get_aws_region()
        )
    return _helpers_service_clients[service_name]


def get_forecast_client():
    """Get the global forecast boto3 client"""
    return get_service_client("forecast")


def get_sts_client():
    """Get the global sts boto3 client"""
    return get_service_client("sts")


def get_sns_client():
    """Get the global sns boto3 client"""
    return get_service_client("sns")


def get_s3_client():
    """Get the global s3 boto3 client"""
    return get_service_client("s3")


def get_quicksight_client():
    """Get the global QuickSight boto3 client"""
    return get_service_client("quicksight")


def get_iam_client():
    """Get the global IAM boto3 client"""
    return get_service_client("iam")


def get_sfn_client():
    """Get the global step functions boto3 client"""
    return get_service_client("stepfunctions")


def get_aws_account_id():
    """
    Get the caller's AWS account ID
    :return: The AWS account ID
    """
    sts_client = get_sts_client()
    return sts_client.get_caller_identity().get("Account")


def get_aws_region():
    """
    Get the caller's AWS region from the environment variable AWS_REGION
    :return: the AWS region name (e.g. us-east-1)
    """
    region = environ.get("AWS_REGION")
    if not region:
        raise EnvironmentVariableError("Missing AWS_REGION environment variable.")

    return region


def get_aws_partition():
    """
    Get the caller's AWS partion by driving it from AWS region
    :return: partition name for the current AWS region (e.g. aws)
    """
    region_name = environ.get("AWS_REGION")
    china_region_name_prefix = "cn"
    us_gov_cloud_region_name_prefix = "us-gov"
    aws_regions_partition = "aws"
    aws_china_regions_partition = "aws-cn"
    aws_us_gov_cloud_regions_partition = "aws-us-gov"

    # China regions
    if region_name.startswith(china_region_name_prefix):
        return aws_china_regions_partition
    # AWS GovCloud(US) Regions
    elif region_name.startswith(us_gov_cloud_region_name_prefix):
        return aws_us_gov_cloud_regions_partition
    else:
        return aws_regions_partition


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

    _tags = {}

    def __init__(self, resource, **resource_creation_kwargs):
        self.account_id = get_aws_account_id()
        self.region = get_aws_region()
        self.cli = get_forecast_client()
        self.resource = resource
        self.validator = InputValidator(
            f"create_{self.resource}", **resource_creation_kwargs
        )
        self.add_tag("SolutionId", SOLUTION_ID)

    def add_tag(self, name: str, value: str):
        """
        Add a tag to the list of tags that this resource should have
        :param name: The name of the tag
        :param value: The value of the tag
        :return: None
        """
        self._tags[name] = value

    @property
    def tags(self):
        return [{"Key": tag, "Value": self._tags.get(tag)} for tag in self._tags]

    def get_service_tag_for_arn(self, arn, name, default=None):
        response = self.cli.list_tags_for_resource(ResourceArn=arn)
        tags = {tag.get("Key"): tag.get("Value") for tag in response.get("Tags")}
        return tags.get(name, default)

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
