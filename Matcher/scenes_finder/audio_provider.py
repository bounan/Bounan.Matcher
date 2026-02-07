import logging
import os
from typing import Iterator

import m3u8
from m3u8 import SegmentList, Segment

from Matcher.config.config import Config
from Matcher.helpers.not_none import not_none
from Matcher.helpers.pre_request import PreRequestQueue
from Matcher.scenes_finder.audio_merger import download_and_merge_parts

logger = logging.getLogger(__name__)


class AudioProvider:
    """Class that downloads and merges audio segments into a single wav file."""

    _DELETE_TEMP_FILES = True  # Set to False for debugging purposes

    _truncated_durations_per_episode: list[float] = []
    _initialized: bool = False
    _completed: bool = False

    _playlists: list[m3u8.M3U8]
    _opening: bool

    def __init__(self, playlists: list[m3u8.M3U8], opening: bool):
        self._playlists = playlists
        self._opening = opening

    def get_iterator(self) -> Iterator[tuple[str, float, float]]:
        """Generator that downloads and merges .wav files for each playlist."""
        assert not self._initialized, "WavProcessor cannot be used twice."
        self._initialized = True

        if len(self._playlists) < 2:
            self._completed = True
            return

        config_dict = Config.export()
        with PreRequestQueue[[m3u8.M3U8, bool, int], tuple[str, float]](config_dict) as queue:
            queue.pre_request(0, self._get_wav, self._playlists[0], self._opening, 0)

            for i, playlist in enumerate(self._playlists):

                # Retrieve previous request result
                wav_path, segments_duration = queue.pop_result(i)
                if i + 1 < len(self._playlists):
                    # Start next download in advance
                    queue.pre_request(i + 1, self._get_wav, self._playlists[i + 1], self._opening, i + 1)

                truncated_duration = min(segments_duration, Config.seconds_to_match)
                offset = 0 if self._opening else max(segments_duration - Config.seconds_to_match, 0)

                self._truncated_durations_per_episode.append(truncated_duration)
                yield wav_path, offset, truncated_duration

                if self._DELETE_TEMP_FILES and os.path.exists(wav_path):
                    os.remove(wav_path)

        self._completed = True

    @property
    def truncated_durations(self) -> list[float]:
        """Returns the list of truncated durations per episode."""
        assert self._completed, "WavProcessor must be completed before calling this method."
        return self._truncated_durations_per_episode

    @staticmethod
    def _get_wav(playlist: m3u8.M3U8, opening: bool, episode: int) -> tuple[str, float]:
        """
        Downloads and merges audio segments into a single wav file.
        Warn: this method is called in separate subprocesses.
        """
        segments, current_duration = AudioProvider._build_segments_list(playlist, opening)
        wav_path = download_and_merge_parts(episode, segments)
        return wav_path, current_duration

    @staticmethod
    def _build_segments_list(playlist: m3u8.M3U8, opening: bool) -> tuple[list[str], float]:
        """
        Builds a list of segment URIs based on whether it's an opening or not.
        Warn: this method is called in separate subprocesses.
        """
        current_duration = 0.0
        segments = []

        segment_iter: list[Segment] = playlist.segments if opening else reversed(playlist.segments)
        for segment in segment_iter:
            uri = not_none(segment.absolute_uri)
            if opening:
                segments.append(uri)
            else:
                segments.insert(0, uri)

            current_duration += not_none(segment.duration)
            if current_duration >= Config.seconds_to_match:
                break

        return segments, current_duration
