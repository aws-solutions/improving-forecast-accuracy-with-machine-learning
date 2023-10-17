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

from constructs import Construct

def is_solution_build(construct: Construct):
    """Detect if this is being run from build-s3-dist.py and should package assets accordingly"""
    solutions_assets_regional = construct.node.try_get_context(
        "SOLUTIONS_ASSETS_REGIONAL"
    )
    solutions_assets_global = construct.node.try_get_context("SOLUTIONS_ASSETS_GLOBAL")
    return solutions_assets_regional and solutions_assets_global
