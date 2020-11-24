# Improving Forecast Accuracy with Machine Learning
## Infrastructure Deployment Tooling

To develop new versions of the solution locally, or to make changes, you can use this package and the AWS CDK to deploy.
   
This README documents the process of developing new versions of this solution.

## Prerequisites

Install the dependencies required to build this stack: 

- Python 3.8
- AWS CDK version 1.70.0
- Docker (builds AWS Lambda functions and layers)
 
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
export QUICKSIGHT_SOURCE="placeholder"

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

If you know that your environment variables (`BUCKET_NAME` and `QUICKSIGHT_SOURCE`) are unlikely to change between 
deployments or environments, you can hardcode them in the `cdk.json` file. 

## 2. Package the solution for release 

It is highly recommended to use CDK to deploy this solution (see step #1 above). While CDK is used to develop the
solution, to package the solution for release as a CloudFormation template use the `build-s3-cdk-dist.py` script:

```
cd <repository_name>/deployment
chmod +x ./build-s3-cdk-dist.py 

export DIST_OUTPUT_BUCKET=my-bucket-name
export SOLUTION_NAME=my-solution-name
export VERSION=my-version
export DIST_ACCOUNT_ID=my-aws-account-id
export DIST_QUICKSIGHT_NAMESPACE=my-quicksight-namespace

./build-s3-cdk-dist.py --source-bucket-name $DIST_OUTPUT_BUCKET --solution-name $SOLUTION_NAME --version-code $VERSION --dist-account-id $DIST_ACCOUNT_ID --dist-quicksight-namespace $DIST_QUICKSIGHT_NAMESPACE 
```

> **Note**: `build-s3-cdk-dist.py` will use your current configured `AWS_REGION` and `AWS_PROFILE`. To set your defaults
install the [AWS Command Line Interface](https://aws.amazon.com/cli/) and run `aws configure`.

#### Parameter Details:
 
- `$DIST_OUTPUT_BUCKET` - This is the global name of the distribution. For the bucket name, the AWS Region is added to
the global name (example: 'my-bucket-name-us-east-1') to create a regional bucket. The lambda artifact should be
uploaded to the regional buckets for the CloudFormation template to pick it up for deployment.
- `$SOLUTION_NAME` - The name of This solution (example: improving-forecast-accuracy-with-machine-learning)
- `$VERSION` - The version number of the change
- `$DIST_ACCOUNT_ID` - The AWS account id from which the Amazon QuickSight templates should be sourced for Amazon
QuickSight Analysis and Dashboard creation
- `$DIST_QUICKSIGHT_NAMESPACE` - The namespace (template prefix) to use together with DIST_ACCOUNT_ID from which the 
Amazon QuickSight template should be sourced for Amazon QuickSight Analysis and Dashboard creation

> **Notes**: The `build_s3_cdk_dist.py` script expects the bucket name as one of its parameters, and this value should 
not include the region suffix. See below on how to create the buckets expected by this solution: 

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
aws s3 sync ./global-s3-assets s3://$DIST_OUTPUT_BUCKET/$SOLUTION_NAME/$VERSION --acl bucket-owner-full-control
aws s3 sync ./regional-s3-assets s3://$DIST_OUTPUT_BUCKET-us-east-1/$SOLUTION_NAME/$VERSION --acl bucket-owner-full-control
```

> **Notes**: Choose your desired region by changing region in the above example from us-east-1 to your desired region 
of the S3 buckets.

## 4. Launch the CloudFormation template

* Get the link of `improving-forecast-accuracy-with-machine-learning.template` uploaded to your Amazon S3 bucket.
* Deploy the solution to your account by launching a new AWS CloudFormation stack using the link of the 
`improving-forecast-accuracy-with-machine-learning.template`.

### Known issues

***

Copyright 2018-2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.