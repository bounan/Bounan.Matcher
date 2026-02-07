using Amazon.CDK;
using Microsoft.Extensions.Configuration;

namespace Bounan.Matcher.AwsCdk;

public class MatcherCdkStackConfig
{
    public MatcherCdkStackConfig(string cdkPrefix)
    {
        var localConfig = new ConfigurationBuilder()
            .AddJsonFile("appsettings.json")
            .AddEnvironmentVariables()
            .Build();

        AlertEmail = GetCdkValue(cdkPrefix, "AlertEmail", localConfig);
        LoanApiFunctionArn = GetCdkValue(cdkPrefix, "LoanApiFunctionArn", localConfig);
        GetSeriesToMatchLambdaName = GetCdkValue(cdkPrefix, "GetSeriesToMatchLambdaName", localConfig);
        UpdateVideoScenesLambdaName = GetCdkValue(cdkPrefix, "UpdateVideoScenesLambdaName", localConfig);
        VideoRegisteredTopicArn = GetCdkValue(cdkPrefix, "VideoRegisteredTopicArn", localConfig);
    }

    public string AlertEmail { get; }

    public string LoanApiFunctionArn { get; }

    public string GetSeriesToMatchLambdaName { get; }

    public string UpdateVideoScenesLambdaName { get; }

    public string VideoRegisteredTopicArn { get; }

    private static string GetCdkValue(string cdkPrefix, string key, IConfigurationRoot localConfig)
    {
        var localValue = localConfig.GetValue<string>(key);
        return localValue is { Length: > 0 } ? localValue : Fn.ImportValue(cdkPrefix + key);
    }
}