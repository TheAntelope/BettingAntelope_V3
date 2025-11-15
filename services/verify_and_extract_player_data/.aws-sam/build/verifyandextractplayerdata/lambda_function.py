import json
import logging
import os
from collections.abc import Mapping

import supabase
import yaml
from yaml.loader import SafeLoader

from antelope_utils.extract_player_stats import refresh_player_info

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

REQUIRED_FIELDS = (
    'PLAYER_NAME',
    'TEAM_NAME',
    'PLAYER_POSITION',
    'currentSeason',
    'status',
)

_supabase_client = None
_meta_data_dict = None


def get_supabase_client():
    global _supabase_client
    if _supabase_client is None:
        supabase_url = os.environ['SUPABASE_URL'].rstrip('/')
        supabase_service_key = os.environ['SUPABASE_SERVICE_KEY']
        _supabase_client = supabase.Client(supabase_url, supabase_service_key)
    return _supabase_client


def get_meta_data():
    global _meta_data_dict
    if _meta_data_dict is None:
        with open('meta_data.yaml') as f:
            _meta_data_dict = yaml.load(f, Loader=SafeLoader)
    return _meta_data_dict


def _deserialize_sqs_record(record: Mapping) -> Mapping:
    body = record.get('body', '')
    if isinstance(body, Mapping):
        payload = body
    else:
        body = str(body or '').strip()
        if not body:
            raise ValueError('SQS message body is empty')
        payload = json.loads(body)
    if isinstance(payload, str):
        payload = json.loads(payload)
    if not isinstance(payload, Mapping):
        raise TypeError('SQS message body must decode to a mapping')
    return payload


def _process_player_event(payload: Mapping, client, meta_data_dict):
    if not isinstance(payload, Mapping):
        raise TypeError('Player payload must be a mapping')

    missing = [field for field in REQUIRED_FIELDS if field not in payload]
    if missing:
        raise KeyError(f"Missing required player fields: {', '.join(missing)}")

    player_name = payload['PLAYER_NAME']
    team_name = payload['TEAM_NAME']
    position = payload['PLAYER_POSITION']
    current_season = int(payload['currentSeason'])
    status = payload['status']

    logger.info(
        'Verifying %s | Team %s | Position %s | Season %s | Status %s',
        player_name,
        team_name,
        position,
        current_season,
        status,
    )

    response = refresh_player_info(
        name=player_name,
        team=team_name,
        position=position,
        currentSeason=current_season,
        client=client,
        meta_data_dict=meta_data_dict,
        status=status,
    )

    logger.debug('Refresh response for %s: %s', player_name, response)
    return response


def lambda_handler(event, context):
    client = get_supabase_client()
    meta_data_dict = get_meta_data()

    if isinstance(event, Mapping) and 'Records' in event:
        records = event.get('Records') or []
        failures = []

        for record in records:
            message_id = record.get('messageId')
            try:
                payload = _deserialize_sqs_record(record)
                _process_player_event(payload, client, meta_data_dict)
            except Exception:  # noqa: BLE001
                logger.exception('Failed to process SQS message %s', message_id)
                if message_id:
                    failures.append({'itemIdentifier': message_id})

        logger.info(
            'Processed %d SQS messages with %d failures',
            len(records),
            len(failures),
        )
        return {'batchItemFailures': failures}

    _process_player_event(event, client, meta_data_dict)
    return 'player stats loaded successfully'


if __name__ == '__main__':
    with open("..\event.json","r",encoding="utf-8") as f:
        sample_event = json.load(f)
    lambda_handler(sample_event, None)
