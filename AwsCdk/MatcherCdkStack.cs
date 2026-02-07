using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using Amazon.CDK;
using Amazon.CDK.AWS.CloudWatch;
using Amazon.CDK.AWS.IAM;
using Amazon.CDK.AWS.Lambda;
using Amazon.CDK.AWS.Logs;
using Amazon.CDK.AWS.SNS;
using Amazon.CDK.AWS.SNS.Subscriptions;
using Amazon.CDK.AWS.SQS;
using Amazon.CDK.AWS.SSM;
using Constructs;
using Newtonsoft.Json;
using AlarmActions = Amazon.CDK.AWS.CloudWatch.Actions;
using LogGroupProps = Amazon.CDK.AWS.Logs.LogGroupProps;

namespace Bounan.Matcher.AwsCdk;

[SuppressMessage("Performance", "CA1859:Use concrete types when possible for improved performance")]
public sealed class MatcherCdkStack : Stack
{
    internal MatcherCdkStack(Construct scope, string id, IStackProps? props = null) : base(scope, id, props)
    {
        var config = new MatcherCdkStackConfig("bounan:");

        var user = new User(this, "User");

        var videoRegisteredQueue = CreateVideoRegisteredQueue(config, user);
        GrantPermissionsForLambdas(config, user);

        var logGroup = CreateLogGroup();
        SetErrorAlarm(config, logGroup);
        SetNoLogsAlarm(config, logGroup);
        logGroup.GrantWrite(user);

        var accessKey = new CfnAccessKey(this, "AccessKey", new CfnAccessKeyProps
        {
            UserName = user.UserName
        });

        var parameter = SaveParameter(logGroup, videoRegisteredQueue, config);
        parameter.GrantRead(user);

        Out("Config", JsonConvert.SerializeObject(config));
        Out(
            "dotenv",
            $"AWS_ACCESS_KEY_ID={accessKey.Ref};\n"
            + $"AWS_SECRET_ACCESS_KEY={accessKey.AttrSecretAccessKey};\n"
            + $"AWS_DEFAULT_REGION={this.Region};\n");
    }

    private IQueue CreateVideoRegisteredQueue(MatcherCdkStackConfig config, IGrantable user)
    {
        var newEpisodesTopic = Topic.FromTopicArn(this, "VideoRegisteredTopic", config.VideoRegisteredTopicArn);
        var newEpisodesQueue = new Queue(this, "VideoRegisteredQueue");
        newEpisodesTopic.AddSubscription(new SqsSubscription(newEpisodesQueue));

        newEpisodesQueue.GrantConsumeMessages(user);

        return newEpisodesQueue;
    }

    private void GrantPermissionsForLambdas(MatcherCdkStackConfig config, IGrantable user)
    {
        var getAnimeToDownloadLambda = Function.FromFunctionName(
            this,
            "GetSeriesToMatchLambda",
            config.GetSeriesToMatchLambdaName);
        getAnimeToDownloadLambda.GrantInvoke(user);

        var updateVideoStatusLambda = Function.FromFunctionName(
            this,
            "UpdateVideoScenesLambda",
            config.UpdateVideoScenesLambdaName);
        updateVideoStatusLambda.GrantInvoke(user);

        var loanApiFunction = Function.FromFunctionAttributes(
            this,
            "LoanApiFunction",
            new FunctionAttributes
            {
                FunctionArn = config.LoanApiFunctionArn,
                SkipPermissions = true
            });
        loanApiFunction.GrantInvoke(user);
    }

    private ILogGroup CreateLogGroup()
    {
        return new LogGroup(this, "LogGroup", new LogGroupProps
        {
            Retention = RetentionDays.ONE_WEEK,
            RemovalPolicy = RemovalPolicy.DESTROY,
        });
    }

    private void SetErrorAlarm(MatcherCdkStackConfig config, ILogGroup logGroup)
    {
        var metricFilter = logGroup.AddMetricFilter("ErrorMetricFilter", new MetricFilterOptions
        {
            FilterPattern = FilterPattern.AnyTerm("ERROR"),
            MetricNamespace = StackName,
            MetricName = "ErrorCount",
            MetricValue = "1",
        });

        var alarm = new Alarm(this, "LogGroupErrorAlarm", new AlarmProps
        {
            Metric = metricFilter.Metric(),
            Threshold = 1,
            EvaluationPeriods = 1,
            TreatMissingData = TreatMissingData.NOT_BREACHING,
        });

        var topic = new Topic(this, "LogGroupAlarmSnsTopic", new TopicProps());
        topic.AddSubscription(new EmailSubscription(config.AlertEmail));
        alarm.AddAlarmAction(new AlarmActions.SnsAction(topic));
    }

    private void SetNoLogsAlarm(MatcherCdkStackConfig config, ILogGroup logGroup)
    {
        var noLogsMetric = new Metric(new MetricProps
        {
            Namespace = "AWS/Logs",
            MetricName = "IncomingLogEvents",
            DimensionsMap = new Dictionary<string, string>
            {
                {
                    "LogGroupName", logGroup.LogGroupName
                }
            },
            Statistic = "Sum",
            Period = Duration.Minutes(2),
        });

        var noLogAlarm = new Alarm(this, "NoLogsAlarm", new AlarmProps
        {
            Metric = noLogsMetric,
            Threshold = 0,
            ComparisonOperator = ComparisonOperator.LESS_THAN_OR_EQUAL_TO_THRESHOLD,
            EvaluationPeriods = 1,
            TreatMissingData = TreatMissingData.BREACHING,
            AlarmDescription = "Alarm if no logs received within 2 minutes"
        });

        var topic = new Topic(this, "NoLogAlarmSnsTopic", new TopicProps());
        topic.AddSubscription(new EmailSubscription(config.AlertEmail));
        noLogAlarm.AddAlarmAction(new AlarmActions.SnsAction(topic));
    }

    private StringParameter SaveParameter(
        ILogGroup logGroup,
        IQueue videoRegisteredQueue,
        MatcherCdkStackConfig config)
    {
        var runtimeConfig = new
        {
            aws_access_key_id = "Should be stored locally",
            aws_secret_access_key = "Should be stored locally",
            loan_api_function_arn = config.LoanApiFunctionArn,
            log_group_name = logGroup.LogGroupName,
            log_level = "INFO",
            min_episode_number = 2,
            episodes_to_match = 5,
            seconds_to_match = 6 * 60,
            notification_queue_url = videoRegisteredQueue.QueueUrl,
            get_series_to_match_lambda_name = config.GetSeriesToMatchLambdaName,
            update_video_scenes_lambda_name = config.UpdateVideoScenesLambdaName,
            temp_dir = "/tmp",
            download_threads = 12,
            download_max_retries_for_ts = 3,
            scene_after_opening_threshold_secs = 4,
            min_scene_length_secs = 20,
            operating_log_rate_per_minute = 1,
            batch_size = 10,
        };

        var json = JsonConvert.SerializeObject(runtimeConfig, Formatting.Indented);

        return new StringParameter(this, "runtime-config", new StringParameterProps
        {
            ParameterName = "/bounan/matcher/runtime-config",
            StringValue = json,
        });
    }

    private void Out(string key, string value)
    {
        _ = new CfnOutput(this, key, new CfnOutputProps
        {
            Value = value
        });
    }
}