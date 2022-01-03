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

import copy
from typing import List, Dict, Optional

from botocore.exceptions import ParamValidationError

from shared.Dataset.data_frequency import DataFrequency
from shared.Dataset.data_timestamp_format import DataTimestampFormat
from shared.Dataset.dataset import Dataset
from shared.Dataset.dataset_domain import DatasetDomain
from shared.Dataset.dataset_file import DatasetFile
from shared.Dataset.dataset_import_job import DatasetImportJob
from shared.Dataset.dataset_type import DatasetType
from shared.DatasetGroup.dataset_group import DatasetGroup
from shared.Forecast.forecast import Forecast
from shared.Predictor.auto_predictor import AutoPredictor
from shared.Predictor.predictor import Predictor
from shared.Tags.tags import validate_tags
from shared.helpers import get_s3_client, get_sfn_client, UserTags
from shared.logging import get_logger

DEFAULT_KEY = "Default"  # Config file defaults under 'Default' section
DEFAULT_S3_KEY = "forecast-defaults.yaml"  # S3 bucket key for forecast defaults
DEFAULT_SFN_KEY = "config"  # StepFunctions input path for config
logger = get_logger(__name__)


class ConfigNotFound(Exception):
    pass


class Config:
    """Used to generate Amazon Forecast resources as specified from a configuration file."""

    def __init__(self):
        self.s3 = get_s3_client()
        self.sfn = get_sfn_client()

    def config_item(  # NOSONAR - higher cognitive complexity allowed
        self, dataset_file: DatasetFile, item: str, config_for=None, default=None
    ):
        """
        Get a configuration item from the configured `config` for the dataset referenced by dataset_file
        :param dataset_file: The dataset file to use as override configuration
        :param item: The config item to get
        :return: The configured config item or default if an override is not specified.
        """
        config = copy.deepcopy(self.config)
        config_for = config_for if config_for else dataset_file.prefix

        # unroll the config for this dataset file type
        for key in config.keys():
            dataset_config = config.get(key).get("Datasets", [])
            if isinstance(dataset_config, dict):
                datasets_from = dataset_config.get("From")
                if not datasets_from:
                    raise ValueError(
                        "datasets must be a list, or a dictionary with a key From: referencing another dataset configuration by name"
                    )
                dataset_config = config.get(datasets_from).get("Datasets", [])

            # set 'Dataset' to the dataset matching the file uploaded
            for dataset in dataset_config:
                if dataset.get("DatasetType") == dataset_file.data_type:
                    config[key]["Dataset"] = dataset

            # ensure Datasets always has the full config
            config[key]["Datasets"] = dataset_config

        override = config.get(config_for, {})
        defaults = config.get(DEFAULT_KEY, {})

        config_filter = item.split(".")

        for key in config_filter:
            override = override.get(key, {})
            defaults = defaults.get(key, {})

        if not override and not defaults:
            if default is not None:
                return default
            else:
                raise ValueError(f"configuration item missing key or value for {item}")

        return override if override else defaults

    def config_tags(
        self, dataset_file: DatasetFile, resource_path: str
    ) -> (List[Dict[str, str]], List[Dict[str, str]]):
        global_tags = self.config_item(dataset_file, "Tags", default=[])
        resource_tags = self.config_item(
            dataset_file, f"{resource_path}.Tags", default=[]
        )
        return UserTags(resource_tags, global_tags)

    def dataset_domain(self, dataset_file: DatasetFile) -> DatasetDomain:
        """
        Get the dataset domain from config
        :param dataset_file: The dataset file to use
        :return: the desired dataset domain
        """
        domain = self.config_item(dataset_file, "Dataset.Domain")
        try:
            domain = DatasetDomain[domain]
        except KeyError as excinfo:
            logger.error(excinfo)
            raise (
                KeyError(f"invalid Dataset.Domain specified for {dataset_file.prefix}")
            )
        return domain

    def dataset_schema(self, dataset_file: DatasetFile) -> dict:
        """
        Get the dataset domain from config
        :param dataset_file: The dataset file to use
        :return: the desired dataset schema
        """
        schema = self.config_item(dataset_file, "Dataset.Schema")
        return schema

    def data_frequency(self, dataset_file: DatasetFile) -> DataFrequency:
        """
        Get the data frequency from config
        :param dataset_file: The dataset file to use
        :return: the desired data frequency
        """
        frequency = self.config_item(dataset_file, "Dataset.DataFrequency")
        return DataFrequency(frequency)

    def data_timestamp_format(
        self, dataset_file: DatasetFile
    ) -> Optional[DataTimestampFormat]:
        """
        Get the data timestamp format from config
        :param dataset_file: The dataset file to use
        :return: the data timestamp format
        """
        # metadata has no timestamp format
        if dataset_file.data_type == DatasetType.ITEM_METADATA:
            return None

        # other datasets have a timestamp format
        _format = self.config_item(dataset_file, "Dataset.TimestampFormat")
        return DataTimestampFormat(_format)

    def data_geolocation_format(self, dataset_file: DatasetFile) -> Optional[str]:
        """
        Get the data geolocation format from config
        :param dataset_file: The dataset file to use
        :return: the data geolocation format
        """
        try:
            _format = self.config_item(dataset_file, "Dataset.GeolocationFormat")
        except ValueError:
            _format = None
        return _format

    def data_time_zone(self, dataset_file: DatasetFile) -> str:
        """
        Get the data timezone from config
        :param dataset_file: The dataset file to use
        :return: the data timezone format
        """
        try:
            _format = self.config_item(dataset_file, "Dataset.TimeZone")
        except ValueError:
            _format = None
        return _format

    def data_geolocation_for_time_zone(self, dataset_file: DatasetFile) -> bool:
        """
        Get whether to use geolocation for timezone from config
        :param dataset_file: The dataset file to use
        :return: whether to use geolocation for timezone
        """
        try:
            use_geolocation = self.config_item(
                dataset_file, "Dataset.UseGeolocationForTimeZone"
            )
        except ValueError:
            use_geolocation = None
        return use_geolocation

    def dataset(self, dataset_file: DatasetFile) -> Dataset:
        """
        Get the dataset from config
        :param dataset_file: The dataset file to use
        :return: the dataset
        """
        """Get the dataset referenced by the dataset file"""
        dataset_parameters = {
            "dataset_name": dataset_file.name,
            "dataset_type": dataset_file.data_type,
            "dataset_domain": self.dataset_domain(dataset_file),
            "dataset_schema": self.dataset_schema(dataset_file),
        }
        if dataset_file.data_type != DatasetType.ITEM_METADATA:
            dataset_parameters["data_frequency"] = self.data_frequency(dataset_file)

        ds = Dataset(
            user_tags=self.config_tags(dataset_file, "Dataset"), **dataset_parameters
        )
        return ds

    def datasets(self, dataset_file: DatasetFile) -> List[Dataset]:
        """
        Get all datasets that would be referenced by a dataset group.
        :param dataset_file: The dataset file to use
        :return: A list of all datasets that are codependent with dataset_file
        """
        required = self.required_datasets(dataset_file)
        dataset_templates = []
        for data_type in required:
            dataset_file.data_type = DatasetType[data_type]
            ds = self.dataset(dataset_file)
            dataset_templates.append(ds)

        return dataset_templates

    def dataset_group_domain(
        self, dataset_file: DatasetFile, dataset_group_name: str = None
    ) -> DatasetDomain:
        """
        Get the dataset group domain
        :param dataset_file: The dataset file to use
        :return: The dataset group domain
        """
        domain = self.config_item(
            dataset_file, "DatasetGroup.Domain", dataset_group_name
        )
        try:
            domain = DatasetDomain[domain]
        except KeyError as excinfo:
            logger.error(excinfo)
            raise (
                KeyError(
                    f"invalid DatasetGroup.Domain specified for {dataset_file.prefix}"
                )
            )
        return domain

    def dependent_dataset_groups(self, dataset_file: DatasetFile) -> List[DatasetGroup]:
        """
        Get the list of all dataset groups that use this dataset file
        :param datset_file:
        :return: The list of all dataset groups that use this dataset file
        """
        dataset_group_name = dataset_file.prefix
        dependent_dsgs = [dataset_group_name]

        for toplevel_item in self.config.keys():
            datasets = self.config.get(toplevel_item).get("Datasets")
            from_datasets = datasets.get("From") if isinstance(datasets, dict) else None

            if from_datasets == dataset_group_name:
                dependent_dsgs.append(toplevel_item)

        return dependent_dsgs

    def dataset_group(self, dataset_file: DatasetFile, dataset_group_name: str = None):
        """
        Get the dataset group
        :param dataset_file: The dataset file to use
        :param dataset_group_name:
        :return: The dataset group
        """

        name = dataset_group_name if dataset_group_name else dataset_file.prefix
        domain = self.dataset_group_domain(dataset_file, dataset_group_name)

        dsg = DatasetGroup(
            dataset_group_name=name,
            dataset_domain=domain,
        )

        ds = self.dataset(dataset_file)
        if ds.dataset_domain != dsg.dataset_group_domain:
            raise ValueError(
                f"The dataset group domain ({dsg.dataset_group_domain}) and dataset domain ({ds.dataset_domain}) must match."
            )

        return dsg

    def dataset_groups(self, dataset_file: DatasetFile) -> List[DatasetGroup]:
        """
        Get the dataset groups that depend on this file
        :param dataset_file: The dataset file to use
        :return: The dataset Groups
        """
        dsg_names = self.dependent_dataset_groups(dataset_file)
        dsgs = []

        logger.info("%s depend on %s" % (", ".join(dsg_names), dataset_file.prefix))
        for dsg_name in dsg_names:
            dsg = DatasetGroup(
                dataset_group_name=dsg_name,
                dataset_domain=self.dataset_group_domain(dataset_file),
                user_tags=self.config_tags(
                    DatasetFile(dsg_name, dataset_file.bucket), "DatasetGroup"
                ),
            )

            ds = self.dataset(dataset_file)
            if ds.dataset_domain != dsg.dataset_group_domain:
                raise ValueError(
                    f"The dataset group domain ({dsg.dataset_group_domain}) and dataset domain ({ds.dataset_domain}) must match."
                )

            dsgs.append(dsg)

        return dsgs

    def dataset_import_job(self, dataset_file: DatasetFile):
        """
        Get the dataset import job
        :param dataset_file: The dataset file to use
        :return: The dataset import job
        """

        ds = self.dataset(dataset_file)
        dsi = DatasetImportJob(
            dataset_file=dataset_file,
            dataset_arn=ds.arn,
            timestamp_format=self.data_timestamp_format(dataset_file),
            geolocation_format=self.data_geolocation_format(dataset_file),
            time_zone=self.data_time_zone(dataset_file),
            use_geolocation_for_time_zone=self.data_geolocation_for_time_zone(
                dataset_file
            ),
            user_tags=self.config_tags(dataset_file, "Dataset"),
        )

        return dsi

    def required_datasets(self, dataset_file: DatasetFile):
        """
        Get the codependent dataset types for this dataset file
        :param dataset_file: The dataset file to use
        :return: A list of dataset types required
        """
        datasets = self.config_item(dataset_file, "Datasets")
        defaults = self.config.get("Default", {}).get("Datasets", None)

        # the default behavior is to require just timeseries data for predictor generation
        # other datasets will be used if they are present, but predictors might be generated
        # before they are added
        if datasets == defaults:
            return ["TARGET_TIME_SERIES"]

        # if the user has provided additional dataset configuration, use the
        # required dataset types defined there instead.
        datasets = [dataset.get("DatasetType") for dataset in datasets]
        if "TARGET_TIME_SERIES" not in datasets:
            raise ValueError(
                f"you must configure a TARGET_TIME_SERIES dataset for {dataset_file.name}"
            )
        if len(list(set(datasets))) != len(datasets):
            raise ValueError(f"duplicate dataset types found on {dataset_file.name}")

        return datasets

    def predictor(self, dataset_file: DatasetFile, dataset_group_name: str):
        """
        Get the predictor
        :param dataset_file: The dataset file to use
        :param dataset_group_name: The name of the dataset group this predictor is being generated for
        :return: The predictor
        """
        auto_predictor_config = self.config_item(
            dataset_file, "AutoPredictor", dataset_group_name, default=False
        )
        predictor_config = self.config_item(
            dataset_file, "Predictor", dataset_group_name, default=False
        )

        if auto_predictor_config:
            predictor_factory = AutoPredictor
            tags_for = "AutoPredictor"
            predictor_config = auto_predictor_config
        elif predictor_config:
            predictor_factory = Predictor
            tags_for = "Predictor"
        else:
            raise ValueError(
                "a Predictor or AutoPredictor configuration must be present"
            )

        dsg = self.dataset_group(dataset_file, dataset_group_name)

        pred = predictor_factory(
            dataset_file=dataset_file,
            dataset_group=dsg,
            user_tags=self.config_tags(dataset_file, tags_for),
            **predictor_config,
        )

        return pred

    def forecast(
        self, dataset_file: DatasetFile, dataset_group_name: str, predictor_arn: str
    ):
        """
        Get the forecast
        :param dataset_file: The dataset file to use
        :return: The forecast
        """
        forecast_config = self.config_item(dataset_file, "Forecast", dataset_group_name)
        dsg = self.dataset_group(dataset_file, dataset_group_name)

        fcst = Forecast(
            predictor_arn=predictor_arn,
            dataset_group=dsg,
            user_tags=self.config_tags(dataset_file, "Forecast"),
            **forecast_config,
        )

        return fcst

    @classmethod
    def from_sfn(cls, event):
        """
        Used to load Config from any AWS Lambda function invoked by the AWS Step Functions State Machine
        :param event: The event passed to the AWS Lambda handler
        :return: Config
        """
        cfg = Config()
        cfg.config = event.get(DEFAULT_SFN_KEY)
        return cfg

    @classmethod
    def from_s3(cls, bucket):
        """
        Used to load Config from S3 (using default key DEFAULT_S3_KEY)
        :param bucket: The bucket to load config from
        :return: Config
        """
        cfg = Config()
        try:
            s3_config = cfg.s3.get_object(Bucket=bucket, Key=DEFAULT_S3_KEY)
        except cfg.s3.exceptions.NoSuchKey:
            raise ConfigNotFound(
                f"Configuration file s3://{bucket}/{DEFAULT_S3_KEY} not found. Please refer to the solutions implementation guide for configuration instructions."
            )

        # try to load the configuration as YAML
        import yaml

        try:
            loaded_cfg = yaml.safe_load(s3_config.get("Body").read().decode("utf-8"))
        except yaml.YAMLError as excinfo:
            raise ValueError(f"{DEFAULT_S3_KEY} is not a valid config file: {excinfo}")

        # make sure the config is a dictionary
        cfg_type = type(loaded_cfg).__name__
        if cfg_type != "dict":
            raise ValueError(
                f"{DEFAULT_S3_KEY} should contain a YAML dict but is a {cfg_type}."
            )

        # make sure the config contains a default key
        default = loaded_cfg.get("Default")
        if not default:
            raise ValueError(f"{DEFAULT_S3_KEY} should contain a `Default` key")

        cfg.config = loaded_cfg
        return cfg

    def _valid_toplevel_keys(self, errors):
        cfg_copy = copy.deepcopy(self.config)
        config_keys = list(cfg_copy.keys())
        if "__Testing__" in config_keys:
            config_keys.remove("__Testing__")

        for key in config_keys:
            if not isinstance(cfg_copy.get(key), dict):
                errors.append(
                    f"configuration file top level key {key} must be a dictionary"
                )

        return config_keys

    def _valid_dataset_group(self, config_key, resource, config_data, errors):
        try:
            DatasetGroup.validate_config(DatasetGroupName="placeholder", **config_data)
        except ParamValidationError as excinfo:
            errors.append(
                f"configuration issue for {config_key}.{resource}: {str(excinfo)}"
            )

    def _valid_datasets(self, config_key, resource, config_data, errors):
        if isinstance(config_data, dict):
            from_datasets = config_data.get("From", None)
            config_data = copy.deepcopy(
                self.config.get(from_datasets, {}).get("Datasets", [])
            )

            if not config_data:
                errors.append(
                    f"Datasets for {config_key} references {from_datasets} but no config found for datasets in that group"
                )
                return  # can do no further validation here.

        if not isinstance(config_data, list):
            errors.append(
                f'Datasets for {config_key} must be a list or a dictionary containing the "From" key'
            )

        for dataset_config in config_data:
            for non_dataset_field in [
                "TimestampFormat",
                "GeolocationFormat",
                "TimeZone",
                "UseGeolocationForTimeZone",
            ]:
                dataset_config.pop(non_dataset_field, None)

            try:
                Dataset.validate_config(DatasetName="placeholder", **dataset_config)
            except ParamValidationError as excinfo:
                errors.append(
                    f"configuration issue for {config_key}.{resource}: {str(excinfo)}"
                )

    def _valid_predictor(self, config_key, resource, config_data, errors):
        config_data.pop("MaxAge", None)

        input_data_config = config_data.pop("InputDataConfig", {})
        input_data_config["DatasetGroupArn"] = "placeholder"
        config_data.pop("InputDataConfig", None)
        try:
            Predictor.validate_config(
                PredictorName="placeholder",
                InputDataConfig=input_data_config,
                **config_data,
            )
        except ParamValidationError as excinfo:
            errors.append(
                f"configuration issue for {config_key}.{resource}: {str(excinfo)}"
            )

    def _valid_autopredictor(self, config_key, resource, config_data, errors):
        config_data.pop("MaxAge", None)

        input_data_config = config_data.pop("DataConfig", {})
        input_data_config["DatasetGroupArn"] = "placeholder"
        config_data.pop("DataConfig", None)
        try:
            AutoPredictor.validate_config(
                PredictorName="placeholder",
                DataConfig=input_data_config,
                **config_data,
            )
        except ParamValidationError as excinfo:
            errors.append(
                f"configuration issue for {config_key}.{resource}: {str(excinfo)}"
            )

    def _valid_forecast(self, config_key, resource, config_data, errors):
        try:
            Forecast.validate_config(
                ForecastName="placeholder", PredictorArn="placeholder", **config_data
            )
        except ParamValidationError as excinfo:
            errors.append(
                f"configuration issue for {config_key}.{resource}: {str(excinfo)}"
            )

    def _valid_tags(self, config_key, resource, config_data, errors):
        try:
            validate_tags(config_data)
        except ValueError as excinfo:
            errors.append(
                f"configuration issue for {config_key}.{resource}: {str(excinfo)}"
            )

    def _valid_subkeys(  # NOSONAR - higher cognitive complexity allowed
        self, config_key, errors
    ):
        cfg_copy = copy.deepcopy(self.config)
        resources = cfg_copy.get(config_key).keys()

        for required in ["DatasetGroup", "Datasets", "Forecast"]:
            if required not in resources:
                errors.append(
                    f"configuration for {config_key} is missing required resource {required}"
                )
        if not any(
            predictor_type in resources
            for predictor_type in ["Predictor", "AutoPredictor"]
        ):
            errors.append(
                f"configuration for {config_key} is missing one of Predictor or AutoPredictor"
            )

        for resource in resources:
            config_data = cfg_copy.get(config_key).get(resource)
            if isinstance(config_data, dict):
                tags = config_data.pop("Tags", None)
                if tags:
                    self._valid_tags(config_key, resource, tags, errors)

            if resource == "DatasetGroup":
                self._valid_dataset_group(config_key, resource, config_data, errors)
            elif resource == "Datasets":
                self._valid_datasets(config_key, resource, config_data, errors)
            elif resource == "Predictor":
                self._valid_predictor(config_key, resource, config_data, errors)
            elif resource == "AutoPredictor":
                self._valid_autopredictor(config_key, resource, config_data, errors)
            elif resource == "Forecast":
                self._valid_forecast(config_key, resource, config_data, errors)
            elif resource == "Tags":
                self._valid_tags(config_key, resource, config_data, errors)
            else:
                errors.append(
                    f"{config_key} resource {resource} is not supported (must be one of 'DatasetGroup', 'Datasets', 'Predictor', 'Forecast')"
                )

    def validate(self):
        errors = []

        config_keys = self._valid_toplevel_keys(errors)
        for config_key in config_keys:
            self._valid_subkeys(config_key, errors)

        return errors
