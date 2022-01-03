# Improving Forecast Accuracy with Machine Learning
## Infrastructure Deployment Tooling

To develop new versions of the solution locally, or to make changes, you can use this package and the AWS CDK to deploy.
   
This README documents the process of developing new versions of this solution.

## Prerequisites

Install the dependencies required to build this stack: 

- Python 3.8 or later
 
## 1. Build the solution for deployment

You can use the [AWS CDK](https://aws.amazon.com/cdk/) to deploy the solution directly

```shell script
# install the Python dependencies 
cd <repository_name>/source/infrastructure 
virtualenv .venv 
source .venv/bin/activate
pip install -r source/requirements-build-and-test.txt

# set environment variables required by the solution
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
cdk deploy --parameters Email="<your_email>" --parameters NotebookDeploy=Yes
``` 

If you know that your environment variables (`BUCKET_NAME`) are unlikely to change between 
deployments or environments, you can put them in the `cdk.json` file. 

## 2. Package the solution for release 

It is highly recommended to use CDK to deploy this solution (see step #1 above). While CDK is used to develop the
solution, to package the solution for release as a CloudFormation template use the `build-s3-cdk-dist` script:

```
cd <repository_name>/deployment 

export BUCKET_NAME=my-bucket-name
export SOLUTION_NAME=my-solution-name
export VERSION=my-version
export DIST_ACCOUNT_ID=my-aws-account-id
export DIST_QUICKSIGHT_NAMESPACE=my-quicksight-namespace

build-s3-cdk-dist deploy --source-bucket-name $BUCKET_NAME --solution-name $SOLUTION_NAME --version-code $VERSION --cdk-app-path ../source/infrastructure/deploy.py --cdk-app-entrypoint deploy:build_app --extra-regional-assets ../source/glue 
```

> **Note**: `build-s3-cdk-dist` will use your current configured `AWS_REGION` and `AWS_PROFILE`. To set your defaults
install the [AWS Command Line Interface](https://aws.amazon.com/cli/) and run `aws configure`.

#### Parameter Details:
 
- `$BUCKET_NAME` - This is the global name of the distribution. For the bucket name, the AWS Region is added to
the global name (example: 'my-bucket-name-us-east-1') to create a regional bucket. The lambda artifact should be
uploaded to the regional buckets for the CloudFormation template to pick it up for deployment.
- `$SOLUTION_NAME` - The name of This solution (example: improving-forecast-accuracy-with-machine-learning)
- `$VERSION` - The version number of the change
- `$DIST_ACCOUNT_ID` - The AWS account id from which the Amazon QuickSight templates should be sourced for Amazon
QuickSight Analysis and Dashboard creation
- `$DIST_QUICKSIGHT_NAMESPACE` - The namespace (template prefix) to use together with DIST_ACCOUNT_ID from which the 
Amazon QuickSight template should be sourced for Amazon QuickSight Analysis and Dashboard creation

> **Notes**: When you define `BUCKET_NAME`, a randomized value is recommended. You will need to create an S3 bucket
> where the name is `<BUCKET_NAME>-<REGION>`. The solution's CloudFormation template will expect the source code to be 
> located in a bucket matching that name.  

## 3. Upload deployment assets to yur Amazon S3 buckets

After the CloudFormation template and regional bucket have been created, you must:

- deploy the distributable to the Amazon S3 bucket in your account. Make sure you are uploading the distributable to the 
`<BUCKET_NAME>-<REGION>` bucket.

## 4. Launch the CloudFormation template

* Get the link of `improving-forecast-accuracy-with-machine-learning.template` uploaded to your Amazon S3 bucket.
* Deploy the solution to your account by launching a new AWS CloudFormation stack using the link of the 
`improving-forecast-accuracy-with-machine-learning.template`.


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