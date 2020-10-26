# Improving Forecast Accuracy with Machine Learning
## Sample Synthetic Data Creation Tooling

In order to test and demo the Improving Forecast Accuracy with Machine Learning Solution, it is useful to have a set of 
synthetic data that represents a time series that a customer would wish to forecast. Many repositories will include 
example datasets, but do not provide a mechanism to generate sample synthetic data. 

This is useful to both

 - generate sample data files that Amazon Forecast can support (so that users can compare to their own data sets)
 - create "predictably random" forecastable datasets (this can help us test the forecast result yields the expected 
   value of our stochastic process)
   
This README documents the process of generating synthetic data with the `create_synthetic_data.py` script.

## Getting Started

The `create_synthetic_data.py` script can be run from the command line: 

```
./create_synthetic_data.py --help
Usage: create_synthetic_data.py [OPTIONS] [INPUT]

  Create synthetic data for the items defined in INPUT (default:
  `config.yaml`)

Options:
  --start TEXT            start date or time, formatted as YYYY-MM-DD or YYYY-
                          MM-DD HH:MM:SS

  --length INTEGER RANGE  number of periods to output for each model defined
                          in the input configuration file

  --plot                  set this flag to output plots of each model
  --help                  Show this message and exit.
```

You will need to prepare a model configuration file (`config.yaml` by default), documented below, to generate data.

## Prerequisites
The following procedures assumes that all of the OS-level configuration has been completed. They are:

* Ensure Python 3.8+ is installed

## 1. Build the solution for deployment

Prepare a Python virtual environment:
```
# ensure Python 3 and virtualenv are installed
cd <repository_name>/source/synthetic
virtualenv .venv
source .venv/bin/activate
pip install -r ../requirements-build-and-test.txt
```

## 2. Prepare the configuration file `config.yaml`

The YAML formatted data file has certain required fields, documented below: 

```
--- 

# this retailer sells penne, two different brands of marinara, and one brand of alfredo sauce across two locations

output: 5min  # required - must be compatible with Amazon Forecast frequencies (Y|M|W|D|30min|15min|10min|5min|1min)   

no_sales_at_night: &no_sales_at_night [0,0,0,0,0,0,0,0,1,1,1,1,1,1,1,1,1,1,1,1,1,1,0,0]  # hourly adjustments must have length 24 (starting at 00:00)
higher_weekends: &higher_weekends [1,1,1,1,1,2,1.5]  # weekly adjustments must have length 7 (starting Monday)
higher_holiday: &higher_holidays [0.5,0.5,1,1,1,1,1,1,1,1,1.5,2.5]  # montly adjustments must have length 12 (starting January)

models:                                 # required     - define each model in a list under this key
  # ottawa location 
  - name: penne x                       # required     - each model requires a name
    rate: 60                            # required     - each model requires a rate (expected occurrences per period)
    per: D                              # required     - period length for rate (must be compatible with Amazon Forecast frequencies (Y|M|W|D|30min|15min|10min|5min|1min))
    dimensions:                         # not required - each model can support optional forecast dimensions (commonly used to break down sales by location) 
      - name: store
        value: ottawa
    metadata:                           # not required - each model can support optional item metadata 
      - name: brand                     #                if metadata is defined, ensure it exists for each model under models
        value: brand x
    seasonalities:                      # not required - seasonalities to apply to the generated data
      hourly: *no_sales_at_night        #              - e.g. many retailers do not sell goods at night
      daily: *higher_weekends           #              - e.g. many retailers have higher sales rates on weekends
      monthly: *higher_holidays         #              - e.g. many retailers have higher sales in some months 
    dependencies:                       # not required - allows us to model complementary goods 
      marinara x:                       # required     - each dependency requires a name (in this case, this model depends on another model called `marinara x`
        chance: .5                      # required     - each dependency requires a chance that a sale of this good results in a sale in the model using it
      marinara y:
        chance: .3
      alfredo y:
        chance: .9

# [...]
```

**Note:** If using forecast dimensions, dependencies will search for the model defined with the same dimensions as 
the one that requires it - in the example above, the dependency on `marinara x` will apply the `marinara x` dependency
to `penne x` where `marinara x` matches the dimensions of `penne x` - that is, a `store` of `ottawa`. 

## 3. Run the script

Once your configuration is complete and saved as `config.yaml` you can run the synthetic data generation tool. The 
example below allows you to generate one year of synthetic data for the configuration defined in the included 
configuration file.

Note the length, `105120 = (60 minutes * 24 hours * 365 days) / 5 minutes = 1 year`. The output frequency is defined in
the configuration file as 5 minutes. Adjust the length to suit your requirements 

`./create_synthetic_data.py --start 2000-01-01 --length 105120` 

## 5. Consume the data

* the dataset generated will be saved by default to ts.csv (it will append to this file)
* the metadata generated will be saved by default to ts.metadata.csv (it will append to this file) 

### Known issues

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