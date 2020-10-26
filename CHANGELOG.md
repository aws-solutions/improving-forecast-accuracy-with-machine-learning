# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2020-10-26
### Added
- Datasets can now be shared between predictor and forecast configurations. As these shared datasets are uploaded, all
 dependent resources are updated in Amazon Forecast. See the Implementation Guide for details.
- Synthetic dataset generation tooling has been improved to support metadata and related time series data. See 
`source/synthetic/README.md` for details.
- After forecast exports are completed, the solution now outputs an Athena table of the same name, combining forecast 
input time series, input metadata, and forecast export into one location. The name of this table matches the name of 
the forecast export in the Amazon Forecast console. These tables are partitioned for data frequencies greater than 
daily using YYYY-MM, and will be output in the Forecast Data bucket under the prefix `exports/<export_name>`
- After forecast exports and Athena tables are created, the solution now automatically creates an interactive and
shareable analysis in Amazon QuickSight with pre-set visuals and configurations for to analyze your forecasts. To enable
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

