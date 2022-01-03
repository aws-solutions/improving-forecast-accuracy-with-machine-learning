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


class Logger:
    """Set up a logger fo this package"""

    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """
        Gets the current logger for this package
        :param name: the name of the logger
        :return: the logger
        """
        logger = logging.getLogger(name)
        if not len(logger.handlers):
            logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter("[%(levelname)s]\t%(name)s\t%(message)s")
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.propagate = False
        return logger
