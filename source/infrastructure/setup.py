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
from pathlib import Path

import setuptools

readme_path = Path(__file__).resolve().parent.parent.parent / "README.md"
with open(readme_path) as fp:
    long_description = fp.read()

cdk_json_path = Path(__file__).resolve().parent / "cdk.json"
cdk_json = json.loads(cdk_json_path.read_text())
VERSION = cdk_json["context"]["SOLUTION_VERSION"]


setuptools.setup(
    name="infrastructure",
    version=VERSION,
    description="AWS CDK stack to deploy the Improving Forecast Accuracy with Machine Learning solution.",
    author="AWS Solutions Builders",
    packages=setuptools.find_packages(),
    install_requires=[
        "aws_cdk_lib>=2.0.0",
        "constructs>=10.2.70",
    ],
    python_requires=">=3.7",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: JavaScript",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Software Development :: Code Generators",
        "Topic :: Utilities",
        "Typing :: Typed",
    ],
)
