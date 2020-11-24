# #####################################################################################################################
#  Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.                                            #
#                                                                                                                     #
#  Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance     #
#  with the License. A copy of the License is located at                                                              #
#                                                                                                                     #
#  http://www.apache.org/licenses/LICENSE-2.0                                                                         #
#                                                                                                                     #
#  or in the 'license' file accompanying this file. This file is distributed on an 'AS IS' BASIS, WITHOUT WARRANTIES  #
#  OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions     #
#  and limitations under the License.                                                                                 #
# #####################################################################################################################
from aws_cdk.aws_lambda import IFunction
from aws_cdk.aws_sns import Subscription, SubscriptionProtocol
from aws_cdk.aws_sns import TopicProps
from aws_cdk.core import (
    Construct,
    CfnParameter,
    CfnCondition,
    Aspects,
)
from aws_solutions_constructs.aws_lambda_sns import LambdaToSns

from interfaces import ConditionalResources


class Notifications(Construct):
    def __init__(
        self,
        scope: Construct,
        id: str,
        lambda_function: IFunction,
        email: CfnParameter,
        email_provided: CfnCondition,
    ):
        super().__init__(scope, id)
        self.topic = self.create_sns(lambda_function)
        self.subscription = self.create_subscription(
            email=email, email_provided=email_provided
        )

    def create_sns(self, lambda_function):
        """
        Create the SNS topic using AWS Solutions Constructs
        :return:
        """
        lambda_sns = LambdaToSns(
            self,
            "NotificationConfiguration",
            existing_lambda_obj=lambda_function,
            topic_props=TopicProps(
                display_name="Improving Forecast Accuracy with Machine Learning Notifications"
            ),
        )
        topic = lambda_sns.sns_topic
        topic.node.default_child.override_logical_id("NotificationTopic")
        return topic

    def create_subscription(self, email, email_provided):
        logical_id = "NotificationSubscription"
        subscription = Subscription(
            self,
            logical_id,
            topic=self.topic,
            endpoint=email.value_as_string,
            protocol=SubscriptionProtocol.EMAIL,
        )
        subscription.node.default_child.override_logical_id(logical_id)
        Aspects.of(subscription).add(ConditionalResources(email_provided))
        return subscription
