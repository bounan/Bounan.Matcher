import logging
import math
import warnings
from statistics import median

import m3u8
import requests
from m3u8 import M3U8
from series_intro_recognizer.config import Config as SirConfig
from series_intro_recognizer.processors.audio_files import recognise_from_audio_files_with_offsets

from Common.py.models import VideoKey, Interval, Scenes
from Matcher.clients.loanapi_client import get_video
from Matcher.config.config import Config
from Matcher.helpers.not_none import not_none
from Matcher.scenes_finder.audio_provider import AudioProvider

logger = logging.getLogger(__name__)


def find_scenes(my_anime_list_id: int,
                dub: str,
                episodes_to_process: list[int]) -> list[tuple[VideoKey, Scenes]]:
    logger.debug("Processing videos")

    playlists_and_durations = [_get_playlist_and_duration(my_anime_list_id, dub, episode)
                               for episode in episodes_to_process]

    empty_playlist_indexes = [i for i, playlist_and_duration in enumerate(playlists_and_durations)
                              if playlist_and_duration is None]
    non_empty_playlists = [playlist_and_duration
                           for _, playlist_and_duration in enumerate(playlists_and_durations)
                           if playlist_and_duration is not None]
    if len(empty_playlist_indexes) > 0:
        logger.warning(f"Skipping empty episodes: {empty_playlist_indexes}")

    found_scenes = _get_scenes_by_playlists(non_empty_playlists)

    all_scenes: list[Scenes] = list(found_scenes)
    for index in empty_playlist_indexes:
        all_scenes.insert(index, Scenes(None, None, None))

    video_keys = [VideoKey(my_anime_list_id, dub, episode)
                  for episode in episodes_to_process]
    result = list(zip(video_keys, all_scenes))

    return result


def _get_scenes_by_playlists(playlists_and_durations: list[tuple[M3U8, float]]) -> list[Scenes]:
    sir_config = SirConfig(series_window=Config.episodes_to_match,
                           save_intermediate_results=False)
    openings = _get_openings(playlists_and_durations, sir_config)
    endings = _get_endings(playlists_and_durations, sir_config)

    result = []
    for (_, total_duration), opening, ending in zip(playlists_and_durations, openings, endings):
        scenes = _combine_scenes(opening, ending, total_duration)
        rounded_scenes = _round_scenes(scenes)
        result.append(rounded_scenes)

    return result


def _get_playlist_and_duration(my_anime_list_id: int,
                               dub: str,
                               episode: int) -> tuple[m3u8.M3U8, float] | None:
    logger.info(f"Getting playlist for episode {episode}")
    playlists = get_video(my_anime_list_id, dub, episode).playlists

    lowest_quality = min(playlists.keys(), key=lambda quality: int(quality))
    lowest_quality_playlist = playlists[lowest_quality]

    playlist = m3u8.load(lowest_quality_playlist)
    if not playlist.segments:
        logger.warning(f"Skipping episode {episode} because it has no segments")
        return None

    total_duration = sum([not_none(segment.duration) for segment in playlist.segments])
    if total_duration < 2 * Config.seconds_to_match:
        logger.warning(f"Skipping episode {episode} because it's too short ({total_duration}s)")
        return None

    return playlist, total_duration


def _get_openings(playlists_and_durations: list[tuple[M3U8, float]], sir_config: SirConfig) -> list[Interval]:
    playlists = [playlist for playlist, _ in playlists_and_durations]
    wav_processor = AudioProvider(playlists, True)
    opening_iter = wav_processor.get_iterator()
    lib_openings = recognise_from_audio_files_with_offsets(opening_iter, sir_config)
    openings = [Interval(opening.start, opening.end) for opening in lib_openings]

    truncated_durations = wav_processor.truncated_durations
    fixed_openings: list[Interval] = _fix_openings(openings, playlists_and_durations, truncated_durations)

    return fixed_openings


def _get_endings(playlists_and_durations: list[tuple[M3U8, float]], sir_config: SirConfig) -> list[Interval]:
    playlists = [playlist for playlist, _ in playlists_and_durations]
    wav_processor = AudioProvider(playlists, False)
    ending_iter = wav_processor.get_iterator()
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        lib_endings = recognise_from_audio_files_with_offsets(ending_iter, sir_config)
        endings = [Interval(ending.start, ending.end) for ending in lib_endings]

    truncated_durations = wav_processor.truncated_durations
    fixed_endings: list[Interval] = _fix_endings(endings, playlists_and_durations, truncated_durations)

    return fixed_endings


def _combine_scenes(opening: Interval, ending: Interval, total_duration: float) -> Scenes:
    """
    1. Set field to None if there is no scene.
    2. Extend scenes to the beginning and the end of the video.
    3. Calculate the scene after the ending.
    """
    new_opening = _valid_or_none(opening)
    new_ending = _valid_or_none(ending)

    # Calculate the scene after the ending or extend the ending.
    scene_after_ending = None
    if new_ending is not None:
        if total_duration - new_ending.end > Config.scene_after_opening_threshold_secs:
            scene_after_ending = Interval(new_ending.end, total_duration)
        else:
            new_ending = Interval(new_ending.start, total_duration)

    return Scenes(new_opening,
                  new_ending,
                  _valid_or_none(scene_after_ending))


def _fix_openings(openings: list[Interval],
                  playlists_and_durations: list[tuple[M3U8, float]],
                  truncated_durations: list[float]) -> list[Interval]:
    """
    Fix openings by extending them to the beginning or prolonging them to the median duration.
    """
    fixed_openings: list[Interval] = []
    zipped = list(zip(openings, truncated_durations, playlists_and_durations))
    median_duration = median([opening.end - opening.start for opening, _, _ in zipped])
    for opening, duration, (_, total_duration) in zipped:
        if opening.start < Config.scene_after_opening_threshold_secs:
            # If the beginning of the opening is close to the beginning of the video, extend it.
            fixed_openings.append(Interval(0, opening.end))
        elif abs(total_duration - opening.end) < Config.scene_after_opening_threshold_secs:
            # If the end of the opening is close to the end of the video, extend it to the average duration.
            fixed_openings.append(Interval(opening.start,
                                           opening.start + median_duration))
        else:
            fixed_openings.append(opening)

    return fixed_openings


def _fix_endings(endings: list[Interval],
                 playlists_and_durations: list[tuple[M3U8, float]],
                 truncated_durations: list[float]) -> list[Interval]:
    fixed_endings: list[Interval] = []
    zipped = list(zip(endings, truncated_durations, playlists_and_durations))
    for ending, duration, (_, total_duration) in zipped:
        # Endings are truncated from the beginning, so we need to offset them.
        offset = total_duration - duration
        fixed_endings.append(Interval(ending.start + offset,
                                      ending.end + offset))

    return fixed_endings


def _valid_or_none(scene: Interval | None) -> Interval | None:
    return (scene
            if (scene is not None
                and not math.isnan(scene.start)
                and not math.isnan(scene.end)
                and scene.end - scene.start >= Config.min_scene_length_secs)
            else None)


def _round_scenes(scenes: Scenes) -> Scenes:
    return Scenes(_round_scene(scenes.opening),
                  _round_scene(scenes.ending),
                  _round_scene(scenes.scene_after_ending))


def _round_scene(scene: Interval | None) -> Interval | None:
    return Interval(round(scene.start, 2), round(scene.end, 2)) if scene else None
