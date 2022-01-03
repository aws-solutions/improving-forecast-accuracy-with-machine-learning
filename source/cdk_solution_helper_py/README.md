# CDK Solution Helper for Python and CDK
## Infrastructure Deployment Tooling

This tooling helps you develop new AWS Solutions using the AWS CDK with an approach that is compatible with the 
current AWS Solutions build pipeline. 
   
This README summarizes using the tool. 

## Prerequisites

Install this package. It requires at least

- Python 3.7
- AWS CDK version 1.95.2 or higher

To install the packages: 

```
pip install <path>/cdk_solution_helper_py/helpers_cdk    # where <path> is the path to the solution helper
pip install <path>/cdk_solution_helper_py/helpers_common # where <path> is the path to the solution helper 
```
 
## 1. Create a new CDK application

```shell script
mkdir -p your_solution_name/deployment 
mkdir -p your_solution_name/source-infrastructure
cd your_solution_name/source/infrastructure
cdk init app --language=python .
```

## 2. Install the package 

```
cd your_solution_name
virtualenv .venv 
source ./.venv/bin/activate
pip install <path>/cdk_solution_helper_py/helpers_cdk    # where <path> is the path to the solution helper
pip install <path>/cdk_solution_helper_py/helpers_common # where <path> is the path to the solution helper
```

# 3. Write CDK code using the helpers 

This might be a file called `app.py` in your CDK application directory

```python
#!/usr/bin/env python3

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
from pathlib import Path

from aws_cdk import core
from aws_cdk.core import CfnParameter, Construct

from aws_solutions.cdk import CDKSolution
from aws_solutions.cdk.stack import SolutionStack
from aws_solutions.cdk.aws_lambda.python.function import SolutionsPythonFunction

# The solution helper build script expects this logger to be used
logger = logging.getLogger("cdk-helper")

# Initialize the CDKSolution helper - it will be used to build the templates in a solution-compatible manner
solution = CDKSolution(cdk_json_path=Path(__file__).parent.absolute() / "cdk.json")


# Inherit from SolutionStack to create a CDK app compatible with AWS Solutions 
class MyStack(SolutionStack):
    def __init__(self, scope: Construct, construct_id: str, description: str, template_filename, **kwargs):
        super().__init__(scope, construct_id, description, template_filename, **kwargs)

        # add some parameters to the stack
        self.solutions_template_options.add_parameter(
            CfnParameter(
                self, "Parameter1", description="This is a detailed parameter description"
            ),
            label="Description 1",
            group="Group1",
        )
        self.solutions_template_options.add_parameter(
            CfnParameter(
                self, "Parameter2", description="This is a detailed parameter description"
            ),
            label="Description 2",
            group="Group1",
        )

        # add any custom metrics to your stack!
        self.metrics.update({"your_custom_metric": "your_custom_metric_value"})
        
        # example of adding an AWS Lambda function for Python 
        SolutionsPythonFunction(
            self,
            "ExampleLambdaFunction",
            entrypoint=Path(__file__).parent.absolute() / "example_function" / "handler.py",
            function="handler"
        )


@solution.context.requires("SOLUTION_NAME")
@solution.context.requires("SOLUTION_ID")
@solution.context.requires("VERSION")
@solution.context.requires("BUCKET_NAME")
def build_app(context):
    """
    This is the main entrypoint to your solution.
    The @solution.context decorators indicate that those are required CDK context variables
    The solution.synthesizer is required as a synthesizer for each solution stack
    """
    app = core.App(context=context)

    # add constructs to your CDK app that are compatible with AWS Solutions
    MyStack(
        scope=app,
        construct_id="stack",
        description="This is a demo AWS Solution CDK stack",
        template_filename="hello-world.template",
        synthesizer=solution.synthesizer,
    )

    return app.synth()


if __name__ == "__main__":
    result = build_app()
```


## 4. Build the solution for deployment

You can use the [AWS CDK](https://aws.amazon.com/cdk/) to deploy the solution directly

```shell script
# install the Python dependencies 
cd <repository_name> 
virtualenv .venv 
source .venv/bin/activate
pip install -r source/requirements-build-and-test.txt

# change into the infrastructure directory
cd source/infrastructure

# set environment variables required by the solution - use your own bucket name here
export BUCKET_NAME="placeholder"

# bootstrap CDK (required once - deploys a CDK bootstrap CloudFormation stack for assets)  
cdk bootstrap --cloudformation-execution-policies arn:aws:iam::aws:policy/AdministratorAccess

# deploy with CDK
cdk deploy
# 
```

At this point, the stack will be built and deployed using CDK - the template will take on default CloudFormation
parameter values. To modify the stack parameters, you can use the `--parameters` flag in CDK deploy - for example:

```shell script
cdk deploy --parameters [...] 
```

## 5. Package the solution for release 

It is highly recommended to use CDK to deploy this solution (see step #1 above). While CDK is used to develop the
solution, to package the solution for release as a CloudFormation template use the `build-s3-cdk-dist` script:

```
cd <repository_name>/deployment

export DIST_OUTPUT_BUCKET=my-bucket-name
export SOLUTION_NAME=my-solution-name
export VERSION=my-version

build-s3-cdk-dist --source-bucket-name $DIST_OUTPUT_BUCKET --solution-name $SOLUTION_NAME --version-code $VERSION --cdk-app-path ../source/infrastructure/app.py --cdk-app-entrypoint  app:build_app --sync 
```

> **Note**: `build-s3-cdk-dist` will use your current configured `AWS_REGION` and `AWS_PROFILE`. To set your defaults
install the [AWS Command Line Interface](https://aws.amazon.com/cli/) and run `aws configure`.

#### Parameter Details:
 
- `$DIST_OUTPUT_BUCKET` - This is the global name of the distribution. For the bucket name, the AWS Region is added to
the global name (example: 'my-bucket-name-us-east-1') to create a regional bucket. The lambda artifact should be
uploaded to the regional buckets for the CloudFormation template to pick it up for deployment.
- `$SOLUTION_NAME` - The name of This solution (example: your-solution-name)
- `$VERSION` - The version number of the change

> **Notes**: The `build-s3-cdk-dist` script expects the bucket name as one of its parameters, and this value should 
not include the region suffix. See below on how to create the buckets expected by this solution:
> 
> The `SOLUTION_NAME`, and `VERSION` variables might also be defined in the `cdk.json` file. 

## 3. Upload deployment assets to yur Amazon S3 buckets

Create the CloudFormation bucket defined above, as well as a regional bucket in the region you wish to deploy. The
CloudFormation template is configured to pull the Lambda deployment packages from Amazon S3 bucket in the region the
template is being launched in. Create a bucket in the desired region with the region name appended to the name of the
bucket. eg: for us-east-1 create a bucket named: ```my-bucket-us-east-1```. 

For example:

```bash 
aws s3 mb s3://my-bucket-name --region us-east-1
aws s3 mb s3://my-bucket-name-us-east-1 --region us-east-1
```

Copy the built S3 assets to your S3 buckets: 

```
use the --sync option of build-s3-cdk-dist to upload the global and regional assets
```

> **Notes**: Choose your desired region by changing region in the above example from us-east-1 to your desired region 
of the S3 buckets.

## 4. Launch the CloudFormation template

* Get the link of `your-solution-name.template` uploaded to your Amazon S3 bucket.
* Deploy the solution to your account by launching a new AWS CloudFormation stack using the link of the 
`your-solution-name.template`.
  
***

Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.