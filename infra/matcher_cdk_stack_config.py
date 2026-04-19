import json
import os
from pathlib import Path

from aws_cdk import Fn


class MatcherCdkStackConfig:
    def __init__(self, cdk_prefix: str) -> None:
        local_config = self._load_local_config()

        self.alert_email = self._get_cdk_value(cdk_prefix, "AlertEmail", local_config)
        self.loan_api_function_arn = self._get_cdk_value(cdk_prefix, "LoanApiFunctionArn", local_config)
        self.get_series_to_match_lambda_name = self._get_cdk_value(
            cdk_prefix,
            "GetSeriesToMatchLambdaName",
            local_config,
        )
        self.update_video_scenes_lambda_name = self._get_cdk_value(
            cdk_prefix,
            "UpdateVideoScenesLambdaName",
            local_config,
        )
        self.video_registered_topic_arn = self._get_cdk_value(cdk_prefix, "VideoRegisteredTopicArn", local_config)

    @staticmethod
    def _load_local_config() -> dict[str, str]:
        config_path = Path(__file__).with_name("appsettings.json")
        with config_path.open(encoding="utf-8") as config_file:
            appsettings = json.load(config_file)

        merged = {key: value for key, value in appsettings.items() if isinstance(value, str)}
        merged.update({key: value for key, value in os.environ.items() if isinstance(value, str)})
        return merged

    @staticmethod
    def _get_cdk_value(cdk_prefix: str, key: str, local_config: dict[str, str]) -> str:
        local_value = local_config.get(key, "")
        return local_value if local_value else Fn.import_value(f"{cdk_prefix}{key}")

    def to_dict(self) -> dict[str, str]:
        return {
            "AlertEmail": self.alert_email,
            "LoanApiFunctionArn": self.loan_api_function_arn,
            "GetSeriesToMatchLambdaName": self.get_series_to_match_lambda_name,
            "UpdateVideoScenesLambdaName": self.update_video_scenes_lambda_name,
            "VideoRegisteredTopicArn": self.video_registered_topic_arn,
        }
