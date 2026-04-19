import logging

from dotenv import load_dotenv
from Common.py.models import VideoKey
from Matcher.config.config import Config
import Matcher.main
from Matcher.matcher_logger import setup_logging

logger = logging.getLogger(__name__)

_anime_keys: list[str] = [
    # "59360#Beloved",
]


def _parse_video_keys(video_keys: list[str]) -> list[list[VideoKey]]:
    grouped_video_keys: dict[tuple[str, str], list[VideoKey]] = {}
    for video_key_str in video_keys:
        my_anime_list_id, dub, episode = video_key_str.split("#")
        group_key = (my_anime_list_id, dub)

        video_key = VideoKey(int(my_anime_list_id), dub, int(episode))
        grouped_video_keys.setdefault(group_key, []).append(video_key)

    return list(grouped_video_keys.values())


def main():
    logger.info("Initializing the configuration...")
    load_dotenv()
    Config.initialize_from_ssm()
    setup_logging()

    grouped_video_keys: list[list[VideoKey]] = _parse_video_keys(_anime_keys)

    for group in grouped_video_keys:
        mal_id = group[0].my_anime_list_id
        dub = group[0].dub
        logger.info(f"Processing {mal_id}#{dub} ({len(group)} episodes)...")

        force_mode = False
        if group[0].episode == -1:
            assert len(group) == 1, "Only one video key with episode=-1 is allowed."
            force_mode = True
        logger.info(f"Force mode: {force_mode}")

        # noinspection PyProtectedMember
        Matcher.main._process_videos(group, force_process_all_season=force_mode)


if __name__ == "__main__":
    main()
