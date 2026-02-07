import json
import logging
from dataclasses import dataclass
from typing import Optional

import boto3
from dataclasses_json import dataclass_json, LetterCase

from Matcher.config.config import Config

logger = logging.getLogger(__name__)


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class GetEpisodesRequest:
    my_anime_list_id: int
    dub: str


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class GetVideoRequest:
    my_anime_list_id: int
    dub: str
    episode: int


@dataclass_json(letter_case=LetterCase.CAMEL)
@dataclass
class GetVideoResponse:
    playlists: dict[str, str]
    thumbnail_url: Optional[str]


def get_episodes(my_anime_list_id: int, dub: str) -> list[int]:
    request = GetEpisodesRequest(my_anime_list_id=my_anime_list_id, dub=dub)
    payload = request.to_json()  # type: ignore

    response = _get_client().invoke(
        FunctionName=Config.loan_api_function_arn,
        InvocationType='RequestResponse',
        Payload=payload
    )
    resp_payload = response['Payload'].read().decode('utf-8')

    response_obj = json.loads(resp_payload)

    return response_obj


def get_video(my_anime_list_id: int, dub: str, episode: int) -> GetVideoResponse:
    request = GetVideoRequest(my_anime_list_id=my_anime_list_id, dub=dub, episode=episode)
    payload = request.to_json()  # type: ignore

    response = _get_client().invoke(
        FunctionName=Config.loan_api_function_arn,
        InvocationType='RequestResponse',
        Payload=payload
    )
    resp_payload = response['Payload'].read().decode('utf-8')

    resp_payload = GetVideoResponse.schema().loads(resp_payload)  # type: ignore

    return resp_payload


def _get_client():
    return boto3.client('lambda')
