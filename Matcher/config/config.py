import json
import os
from typing import TypeVar

from Matcher.clients import ssm_client

T = TypeVar("T")


def _add_name(func):
    def wrapper(self):
        return func(self, func.__name__)

    return wrapper


class _Config:
    _configuration: dict[str, str] = {}

    def initialize_from_ssm(self) -> None:
        PARAMETER_NAME = os.environ.get('CONFIGURATION_PARAMETER_NAME')
        assert PARAMETER_NAME is not None, "CONFIGURATION_PARAMETER_NAME is not set."
        runtime_config_json = ssm_client.get_ssm_parameter(PARAMETER_NAME)
        self.initialize_from_dict(json.loads(runtime_config_json))

    def initialize_from_dict(self, configuration: dict[str, str]) -> None:
        self._configuration.clear()
        self._configuration.update(configuration)

    def export(self) -> dict[str, str]:
        assert self._configuration is not None, "Configuration is not initialized."
        return dict(self._configuration)

    @property
    @_add_name
    def loan_api_function_arn(self, name: str = "") -> str:
        return self._get_value(name)

    @property
    @_add_name
    def log_group_name(self, name: str = "") -> str:
        return self._get_value(name)

    @property
    @_add_name
    def log_level(self, name: str = "") -> str:
        return self._get_value(name, 'INFO')

    @property
    @_add_name
    def min_episode_number(self, name: str = "") -> int:
        """ Minimum episode number to process. """
        return int(self._get_value(name))

    @property
    @_add_name
    def episodes_to_match(self, name: str = "") -> int:
        """ Number of episodes to match cross each other. """
        return int(self._get_value(name, 5))

    @property
    @_add_name
    def seconds_to_match(self, name: str = "") -> int:
        return int(self._get_value(name, 6 * 60))

    @property
    @_add_name
    def notification_queue_url(self, name: str = "") -> str:
        return self._get_value(name)

    @property
    @_add_name
    def get_series_to_match_lambda_name(self, name: str = "") -> str:
        return self._get_value(name)

    @property
    @_add_name
    def update_video_scenes_lambda_name(self, name: str = "") -> str:
        return self._get_value(name)

    @property
    @_add_name
    def temp_dir(self, name: str = "") -> str:
        return self._get_value(name, '/tmp')

    @property
    @_add_name
    def download_threads(self, name: str = "") -> int:
        return int(self._get_value(name, 12))

    @property
    @_add_name
    def download_max_retries_for_ts(self, name: str = "") -> int:
        return int(self._get_value(name, 3))

    @property
    @_add_name
    def scene_after_opening_threshold_secs(self, name: str = "") -> int:
        return int(self._get_value(name, 4))

    @property
    @_add_name
    def min_scene_length_secs(self, name: str = "") -> int:
        return int(self._get_value(name, 20))

    @property
    @_add_name
    def operating_log_rate_per_minute(self, name: str = "") -> int:
        return int(self._get_value(name, 1))

    @property
    @_add_name
    def batch_size(self, name: str = "") -> int:
        # It will automatically expand up to N*2-1=19 if the last batch contains
        # less than N videos (N:10 -> B:19).
        return int(self._get_value(name, 10))

    def _get_value(self, key: str, default: T | None = None) -> str:
        assert self._configuration is not None, "Configuration is not initialized."
        value = os.environ.get(key) or self._configuration.get(key, str(default))
        assert value is not None, f"Configuration value for '{key}' is not set."
        return value


Config = _Config()
