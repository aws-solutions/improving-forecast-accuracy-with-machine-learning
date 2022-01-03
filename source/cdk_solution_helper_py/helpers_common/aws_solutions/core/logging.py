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

import logging
import os

DEFAULT_LEVEL = "WARNING"


def get_level():
    """
    Get the logging level from the LOG_LEVEL environment variable if it is valid. Otherwise set to WARNING
    :return: The logging level to use
    """
    valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    requested_level = os.environ.get("LOG_LEVEL", DEFAULT_LEVEL)

    if requested_level and requested_level in valid_levels:
        return requested_level

    return DEFAULT_LEVEL


def get_logger(name):
    """
    Get a configured logger. Compatible with both the AWS Lambda runtime (root logger) and local execution
    :param name: The name of the logger (most often __name__ of the calling module)
    :return: The logger to use
    """
    logger = None

    # first case: running as a lambda function or in pytest with conftest
    # second case: running a single test or locally under test
    if len(logging.getLogger().handlers) > 0:
        logger = logging.getLogger()
        logger.setLevel(get_level())

        # overrides
        logging.getLogger("boto3").setLevel(logging.WARNING)
        logging.getLogger("botocore").setLevel(logging.WARNING)
        logging.getLogger("urllib3").setLevel(logging.WARNING)
    else:
        # fmt: off
        logging.basicConfig(level=get_level())  # NOSONAR - log level is user-specified; logs to stdout for AWS Lambda
        # fmt: on
        logger = logging.getLogger(name)

    return logger
