import asyncio
import logging
import os
import time

import ffmpeg  # type: ignore
from aiohttp import ClientSession
from retry.api import retry_call

from Matcher.config.config import Config

logger = logging.getLogger(__name__)


def download_and_merge_parts(index: int, urls: list[str], files_dir: str) -> str:
    """
    Downloads video parts from urls and merges them into a single wav file
    """
    started_at = time.time()
    local_paths = _download_parts(urls, files_dir)
    logger.debug("Downloaded all video parts.")

    playlist_path = _create_playlist_file(local_paths, files_dir)
    logger.debug("Created playlist file.")

    output_path = _merge_parts(index, playlist_path, files_dir)
    logger.debug("Merged video parts into wav file.")

    logger.info(f"Finished in {time.time() - started_at:.2f}s")

    return os.path.normpath(output_path)


def _download_parts(urls: list[str], files_dir: str) -> list[str]:
    logger.debug("Downloading video parts...")
    return asyncio.run(_download_all_files(urls, files_dir))


def _create_playlist_file(parts_paths: list[str], files_dir: str) -> str:
    playlist_path = os.path.join(files_dir, 'playlist.txt')
    with open(playlist_path, 'w') as f:
        for part in parts_paths:
            f.write(f'file {part.replace("\\", "/")}\n')
    logger.debug(f"Created playlist file at {playlist_path}")

    return playlist_path


def _merge_parts(index: int, playlist_path: str, files_dir: str) -> str:
    """
    Merges video parts into a single wav file
    """
    output_path = os.path.join(files_dir, f'{index}.wav')
    (ffmpeg
     .input(playlist_path, format='concat', safe=0)
     .output(output_path, ac=1, ar=44100, format='wav', y=None, loglevel="quiet")
     .run(overwrite_output=True))
    logger.debug(f"Merged video parts into {output_path}")

    return output_path


async def _download_all_files(urls: list[str], files_dir: str) -> list[str]:
    sem = asyncio.Semaphore(Config.download_threads)

    async with ClientSession() as session:
        tasks = [_download_part_retried(sem, session, i, url, files_dir) for i, url in enumerate(urls)]
        results = await asyncio.gather(*tasks)
        return results


async def _download_part_retried(sem: asyncio.Semaphore, session: ClientSession,
                                 index: int, url: str, files_dir: str) -> str:
    return await retry_call(_download_part,
                            fkwargs={'sem': sem, 'session': session,
                                     'index': index, 'url': url, 'files_dir': files_dir},
                            tries=Config.download_max_retries_for_ts,
                            delay=1,
                            logger=logger)


async def _download_part(sem: asyncio.Semaphore, session: ClientSession, index: int, url: str, files_dir: str) -> str:
    async with sem:
        async with session.get(url) as response:
            file_path = os.path.join(files_dir, f'{index}.ts')
            with open(file_path, 'wb') as f:
                async for data in response.content.iter_chunked(1024 * 1024):
                    f.write(data)
            logger.debug(f"Downloaded {url} -> {file_path}")
            return file_path
