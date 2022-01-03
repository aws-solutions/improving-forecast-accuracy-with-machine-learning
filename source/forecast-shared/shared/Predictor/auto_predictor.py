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
from shared.Dataset.dataset_file import DatasetFile
from shared.DatasetGroup.dataset_group import DatasetGroup
from shared.Predictor.predictor import Predictor


class AutoPredictor(Predictor):
    is_auto_predictor = True

    def __init__(
        self, dataset_file: DatasetFile, dataset_group: DatasetGroup, **predictor_config
    ):
        super().__init__(dataset_file, dataset_group, **predictor_config)
