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

from aws_cdk import core as cdk

import forecast.sagemaker.notebook
import quicksight
from aws_solutions.cdk import CDKSolution
from forecast.stack import ForecastStack

logger = logging.getLogger("cdk-helper")
solution = CDKSolution(cdk_json_path=Path(__file__).parent.absolute() / "cdk.json")


@solution.context.requires("SOLUTION_NAME")
@solution.context.requires("SOLUTION_ID")
@solution.context.requires("SOLUTION_VERSION")
@solution.context.requires("BUCKET_NAME")
@solution.context.requires("NOTEBOOKS", forecast.sagemaker.notebook.context)
def build_app(context):
    app = cdk.App(context=context)

    ForecastStack(
        app,
        "forecast-stack-cdk",
        description=f"Automate Amazon Forecast predictor and forecast generation and visualize forecasts via Amazon QuickSight or an Amazon SageMaker Jupyter Notebook",
        template_filename="improving-forecast-accuracy-with-machine-learning.template",
        synthesizer=solution.synthesizer,
        extra_mappings=quicksight.TemplateSource(
            solution_name=context.get("SOLUTION_NAME"),
            solution_version=context.get("SOLUTION_VERSION"),
        ).mappings,
    )

    return app.synth()


if __name__ == "__main__":
    build_app()
