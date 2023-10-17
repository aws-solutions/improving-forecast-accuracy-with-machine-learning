# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.5.5] - 2023-10-13
- Fixed SonarQube security findings.
- Updated inaccurate README deployment instructions.
- Updated 3rd party libraries for compatibility.
- Upgrade to CDK v2.

## [1.5.4] - 2023-04-18
### Updated
- The bucket policy on the logging bucket to grant access to the logging service principal (logging.s3.amazonaws.com) for access log delivery.
- Python libraries.

## [1.5.3] - 2023-03-22
### Fixed
- [Issue #24](https://github.com/aws-solutions/improving-forecast-accuracy-with-machine-learning/issues/24).
### Updated
- Python libraries.

## [1.5.2] - 2023-01-10
### Updated
- Python libraries.
### Removed
- Use of python future library

## [1.5.1] - 2022-12-19
### Fixed
- Fixes [Issue #19](https://github.com/aws-solutions/improving-forecast-accuracy-with-machine-learning/issues/19).

## [1.5.0] - 2022-12-05
### Added
- [Service Catalog AppRegistry](https://docs.aws.amazon.com/servicecatalog/latest/arguide/intro-app-registry.html) 
resource to register the CloudFormation template and underlying resources as an application in both Service Catalog 
AppRegistry and AWS Systems Manager Application Manager.
### Updated
- CDK version 1.173.0

## [1.4.1] - 2021-08-23
### Fixed
- Upgraded Python package versions

## [1.4.0] - 2021-12-22
### Added
- AutoPredictor support has been added.
- Resources can now be tagged using arbitrary, user-provided tags.
### Changed
- The notebook instance now uses the platform `notebook-al2-v1`. 
- Upgrading from earlier versions of the stack is not supported. Please redeploy the stack and copy your configuration
and data to the newly created forecast data bucket.

## [1.3.3] - 2021-06-23
### Changed
- The default notebook instance type is now `ml.t3.medium` for new stacks to improve performance and availability.
### Fixed
- Fixed an issue that might cause the stack to fail to deploy. This failure occurred when AWS::S3::Bucket 
`ForecastBucket` deployed before a required AWS::Lambda::Permission.

## [1.3.2] - 2021-06-17
### Changed
- Amazon Athena engine version 2 is now the default requested workgroup version.

## [1.3.1] - 2021-04-08
### Fixed
- Upgraded Python package versions and removed unused packages

## [1.3.0] - 2021-03-02
### Added
- The solution now supports the [Amazon Forecast Weather Index](https://aws.amazon.com/blogs/machine-learning/amazon-forecast-weather-index-automatically-include-local-weather-to-increase-your-forecasting-model-accuracy/).
For usage examples, reference the [Implementation Guide](https://docs.aws.amazon.com/solutions/latest/improving-forecast-accuracy-with-machine-learning/welcome.html).
- The solution now includes a demo stack (forecasting the number of NYC City taxi pickups in the next 1 hour in each of
260 pickup zones). The demo features AWS managed weather data and holiday features.
### Changed
- The solution now uses an AWS Glue version 2.0 Spark job to transform and aggregate forecast input data and metadata, 
predictor backtest export data and forecast export data for use in Amazon Athena and Amazon QuickSight for query and
visualization.
### Fixed 
- The solution now supports [Amazon Forecast Holiday Calendars](https://aws.amazon.com/about-aws/whats-new/2020/08/amazon-forecast-adds-holiday-calendars-for-66-countries/) 
to help you improve forecast accuracy for items sensitive to holiday demand.

## [1.2.0] - 2020-11-23
### Added
- Implementation now uses [AWS Cloud Development Kit (AWS CDK)](https://aws.amazon.com/cdk/)
and architecture patterns from [AWS Solutions Constructs](https://aws.amazon.com/solutions/constructs/) to create the 
AWS CloudFormation template. See `source/infrastructure/README.md` for details.
### Fixed
- The AWS Step Functions State Machine now tags all dataset files uploaded to Amazon Forecast using a full checksum
rather than the S3 entity tag. This provides better support for multipart uploads and larger datasets. 

## [1.1.0] - 2020-10-26
### Added
- Datasets can now be shared between predictor and forecast configurations. As these shared datasets are uploaded, all
 dependent resources are updated in Amazon Forecast. See the 
 [Implementation Guide](https://docs.aws.amazon.com/solutions/latest/improving-forecast-accuracy-with-machine-learning/welcome.html)
 for details.
- Synthetic dataset generation tooling has been improved to support metadata and related time series data. See 
`source/synthetic/README.md` for details.
- After forecast exports are completed, the solution now outputs an Athena table of the same name, combining forecast 
input time series, input metadata, and forecast export into one location. The name of this table matches the name of 
the forecast export in the Amazon Forecast console. These tables are partitioned for data frequencies greater than 
daily using YYYY-MM, and will be output in the Forecast Data bucket under the prefix `exports/<export_name>`
- After forecast exports and Athena tables are created, the solution now automatically creates an interactive and
shareable analysis in Amazon QuickSight with pre-set visuals and configurations to analyze your forecasts. To enable
this functionality, provide the QuickSight Analysis Owner ARN parameter in CloudFormation and ensure that QuickSight
Edition is enabled in your account and region where this solution is deployed.
### Fixed
- The step function now gracefully handles Amazon Forecast rate limiting. Should you need to run a large number of 
concurrent experiments (e.g. training more than three predictors/ running more than three forecasts simultaneously), 
it is recommended that you request a default quota increase with AWS support.  

## [1.0.1] - 2020-08-07
### Fixed
- fixed a known issue causing multiple dataset upload to fail
- fixed an issue where users would be notified multiple times that a forecast had completed successfully   
### Changed
- anonymous solution metrics publish on all stack changes
- improved update emails as datasets are uploaded

## [1.0.0] - 2020-07-02
### Added
- initial release

