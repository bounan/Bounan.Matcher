import json

from aws_cdk import (
    CfnOutput,
    Duration,
    Fn,
    RemovalPolicy,
    Stack,
    aws_cloudwatch as cloudwatch,
    aws_cloudwatch_actions as cloudwatch_actions,
    aws_iam as iam,
    aws_lambda as lambda_,
    aws_logs as logs,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subscriptions,
    aws_sqs as sqs,
    aws_ssm as ssm,
)
from constructs import Construct

from infra.matcher_cdk_stack_config import MatcherCdkStackConfig


class MatcherCdkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        config = MatcherCdkStackConfig("bounan:")
        user = iam.User(self, "User")

        video_registered_queue = self._create_video_registered_queue(config, user)
        self._grant_permissions_for_lambdas(config, user)

        log_group = self._create_log_group()
        self._set_error_alarm(config, log_group)
        self._set_no_logs_alarm(config, log_group)
        log_group.grant_write(user)

        access_key = iam.CfnAccessKey(
            self,
            "AccessKey",
            user_name=user.user_name,
        )

        parameter = self._save_parameter(log_group, video_registered_queue, config)
        parameter.grant_read(user)

        self._out("Config", json.dumps(config.to_dict()))
        self._out(
            "dotenv",
            f"AWS_ACCESS_KEY_ID={access_key.ref};\n"
            f"AWS_SECRET_ACCESS_KEY={access_key.attr_secret_access_key};\n"
            f"AWS_DEFAULT_REGION={self.region};\n",
        )

    def _create_video_registered_queue(
            self,
            config: MatcherCdkStackConfig,
            user: iam.IGrantable,
    ) -> sqs.IQueue:
        new_episodes_topic = sns.Topic.from_topic_arn(
            self,
            "VideoRegisteredTopic",
            config.video_registered_topic_arn,
        )
        new_episodes_queue = sqs.Queue(self, "VideoRegisteredQueue")
        new_episodes_topic.add_subscription(sns_subscriptions.SqsSubscription(new_episodes_queue))

        new_episodes_queue.grant_consume_messages(user)
        return new_episodes_queue

    def _grant_permissions_for_lambdas(
            self,
            config: MatcherCdkStackConfig,
            user: iam.IGrantable,
    ) -> None:
        get_anime_to_download_lambda = lambda_.Function.from_function_name(
            self,
            "GetSeriesToMatchLambda",
            config.get_series_to_match_lambda_name,
        )
        get_anime_to_download_lambda.grant_invoke(user)

        update_video_status_lambda = lambda_.Function.from_function_name(
            self,
            "UpdateVideoScenesLambda",
            config.update_video_scenes_lambda_name,
        )
        update_video_status_lambda.grant_invoke(user)

        loan_api_function = lambda_.Function.from_function_attributes(
            self,
            "LoanApiFunction",
            function_arn=config.loan_api_function_arn,
            skip_permissions=True,
        )
        loan_api_function.grant_invoke(user)

    def _create_log_group(self) -> logs.ILogGroup:
        return logs.LogGroup(
            self,
            "LogGroup",
            retention=logs.RetentionDays.ONE_WEEK,
            removal_policy=RemovalPolicy.DESTROY,
        )

    def _set_error_alarm(self, config: MatcherCdkStackConfig, log_group: logs.ILogGroup) -> None:
        metric_filter = log_group.add_metric_filter(
            "ErrorMetricFilter",
            filter_pattern=logs.FilterPattern.any_term("ERROR"),
            metric_namespace=self.stack_name,
            metric_name="ErrorCount",
            metric_value="1",
        )

        alarm = cloudwatch.Alarm(
            self,
            "LogGroupErrorAlarm",
            metric=metric_filter.metric(),
            threshold=1,
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.NOT_BREACHING,
        )

        topic = sns.Topic(self, "LogGroupAlarmSnsTopic")
        topic.add_subscription(sns_subscriptions.EmailSubscription(config.alert_email))
        alarm.add_alarm_action(cloudwatch_actions.SnsAction(topic))

    def _set_no_logs_alarm(self, config: MatcherCdkStackConfig, log_group: logs.ILogGroup) -> None:
        no_logs_metric = cloudwatch.Metric(
            namespace="AWS/Logs",
            metric_name="IncomingLogEvents",
            dimensions_map={"LogGroupName": log_group.log_group_name},
            statistic="Sum",
            period=Duration.minutes(2),
        )

        no_log_alarm = cloudwatch.Alarm(
            self,
            "NoLogsAlarm",
            metric=no_logs_metric,
            threshold=0,
            comparison_operator=cloudwatch.ComparisonOperator.LESS_THAN_OR_EQUAL_TO_THRESHOLD,
            evaluation_periods=1,
            treat_missing_data=cloudwatch.TreatMissingData.BREACHING,
            alarm_description="Alarm if no logs received within 2 minutes",
        )

        topic = sns.Topic(self, "NoLogAlarmSnsTopic")
        topic.add_subscription(sns_subscriptions.EmailSubscription(config.alert_email))
        no_log_alarm.add_alarm_action(cloudwatch_actions.SnsAction(topic))

    def _save_parameter(
            self,
            log_group: logs.ILogGroup,
            video_registered_queue: sqs.IQueue,
            config: MatcherCdkStackConfig,
    ) -> ssm.StringParameter:
        runtime_config = {
            "aws_access_key_id": "Should be stored locally",
            "aws_secret_access_key": "Should be stored locally",
            "loan_api_function_arn": config.loan_api_function_arn,
            "log_group_name": log_group.log_group_name,
            "log_level": "INFO",
            "min_episode_number": 2,
            "episodes_to_match": 5,
            "seconds_to_match": 6 * 60,
            "notification_queue_url": video_registered_queue.queue_url,
            "get_series_to_match_lambda_name": config.get_series_to_match_lambda_name,
            "update_video_scenes_lambda_name": config.update_video_scenes_lambda_name,
            "temp_dir": "/tmp",
            "download_threads": 12,
            "download_max_retries_for_ts": 3,
            "scene_after_opening_threshold_secs": 4,
            "min_scene_length_secs": 20,
            "operating_log_rate_per_minute": 1,
            "batch_size": 10,
        }
        json_value = json.dumps(runtime_config, indent=2)

        return ssm.StringParameter(
            self,
            "runtime-config",
            parameter_name="/bounan/matcher/runtime-config",
            string_value=json_value,
        )

    def _out(self, key: str, value: str) -> None:
        CfnOutput(self, key, value=value)
