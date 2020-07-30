# Improving Forecast Accuracy with Machine Learning

The Improving Forecast Accuracy with Machine Learning solution is designed to help organizations that rely on generating accurate forecasts and store historical demand time series data. Whether organizations are developing forecasts for the first time, or optimizing their current pipeline, this solution will reduce the overhead cost of generating forecasts from time series data, related time series data, and item metadata.

This solution supports multiple forecasts and per-forecast parameter configuration in order to reduce the repetitive task of generating multiple forecasts. The use of an AWS Step Function eliminates the undifferentiated heavy lifting of creating Amazon Forecast datasets, dataset groups, predictors, and forecasts—allowing developers and data scientists to focus on the accuracy of their forecasts. Amazon Forecast predictors and forecasts can be updated as item demand data, related timeseries data, and item metadata are refreshed, which allows for A/B testing against different sets of related timeseries data and item metadata. 

As predictors are generated, their accuracy is tracked over time in Amazon CloudWatch, which allows users to track forecast accuracy and identify drifts over multiple forecasts and parameter-tuning configurations. 

To better capture and alert users of data quality issues, a configurable alert function can also be deployed with Amazon Simple Notification Service (Amazon SNS). This notifies the user on success and failure of the automated forecasting job, reducing the need for users to monitor their forecast workflow. 

This guide provides infrastructure and configuration information for planning and deploying the solution in the AWS Cloud.


## Architecture
The following describes the architecture of the solution:

![architecture](source/images/Forecast.jpg)

The AWS CloudFormation template deploys the resources required to automate your Amazon Forecast usage and deployments. Based on the capabilities of the solution, the architecture is divided into three parts: Data Preparation, Forecasting, and Data Visualization. The template includes the includes the following components:

- An Amazon Simple Storage Service (Amazon S3) bucket for Amazon Forecast configuration where you specify configuration settings for your dataset groups, datasets, predictors and forecasts, as well as the datasets themselves.
- An Amazon S3 event notification that triggers when new datasets are uploaded to the related Amazon S3 bucket.
- An AWS Step Functions State Machine. This combines a series of AWS Lambda functions that build, train. and deploy your Machine Learning (ML) models in Amazon Forecast.
- An Amazon Simple Notification Service (Amazon SNS) topic and email subscription that notify an administrator user with the results of the AWS Step Function.
- An optional Amazon SageMaker Notebook Instance that data scientists and developers can use to prepare and process data, and evaluate your Forecast output.


## Getting Started

You can launch this solution with one click from [AWS Solutions Implementations](https://aws.amazon.com/solutions/implementations). To customize the solution, or to contribute to the solution, follow the steps below:

## Prerequisites
The following procedures assumes that all of the OS-level configuration has been completed. They are:

* [AWS Command Line Interface](https://aws.amazon.com/cli/)
* Python 3.7 or later

## 1. Build the solution

Clone this git repository

`git clone https://github.com/awslabs/<repository_name>`

## 2. Build the solution for deployment

Prepare a Python virtual environment:
```
# ensure Python 3 and virtualenv are installed
cd <repository_name>
virtualenv .venv
source .venv/bin/activate
pip install -r source/requirements-build-and-test.txt
```

Build the distributable (using the above configured Python virtual environment):
```
DIST_OUTPUT_BUCKET=my-bucket-name  # S3 bucket name where customized code will reside
SOLUTION_NAME=my-solution-name     # customized solution name
VERSION=my-version                 # version number for the customized code
cd ./deployment 
chmod +x ./build-s3-dist.py
./build-s3-dist.py --source-bucket-name $DIST_OUTPUT_BUCKET --solution-name $SOLUTION_NAME --version-code $VERSION
```

> **Notes**: The _build-s3-dist_ script expects the bucket name as one of its parameters, and this value should not include the region suffix.

## 3. Upload deployment assets to your Amazon S3 buckets

Create the CloudFormation bucket defined above, as well as a regional bucket in the region you wish to deploy. 
The CloudFormation template is configured to pull the Lambda deployment packages from Amazon S3 bucket in the region the template is being launched in. Create a bucket in the desired region with the region name appended to the name of the bucket. eg: for us-east-1 create a bucket named: ```my-bucket-us-east-1```. 

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

 > **Notes**: Choose your desired region by changing region in the above example from us-east-1 to your desired region of the S3 buckets.

## 5. Launch the CloudFormation template

* Get the link of `improving-forecast-accuracy-with-machine-learning.template` uploaded to your Amazon S3 bucket.
* Deploy the solution to your account by launching a new AWS CloudFormation stack using the link of the `improving-forecast-accuracy-with-machine-learning.template`.

### Known issues

- Occasionally, when uploading multiple datasets (or replacing the same dataset) for a new dataset group, that dataset 
group will fail to create successfully. If this occurs, you should receive an email containing an error 
“ResourceAlreadyExistsException” and the state machine will fail. To work around this issue, either upload the failing 
dataset again, or re-run the failed state machine execution by selecting the failed execution in the AWS Step Functions
console, and clicking “New execution,” keeping the input as is.

***

Copyright 2018-2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.