import asyncio
from redis import asyncio as aioredis
from aio_periodic import Transport, Worker
from aio_periodic import rsp
import aiomqtt
import base64
from config import host, port, username, password, periodic_port, redis_host
import json

from typing import Any, Optional

from dtu import (ReqMap, Request, apply_transform, parse_transform_result,
                 process_mqtt_message)
import logging

logger = logging.getLogger('dtu')

worker = Worker()
req_map: Optional[ReqMap] = None
mqtt: Optional[aiomqtt.Client] = None
REDIS_KEY_PREFIX = 'modbridge-edge:'
REDIS_REQ_TTL = 20


def to_str(v: bytes | bytearray | str) -> str:
    if isinstance(v, (bytes, bytearray)):
        return str(v, 'utf-8')
    return v


def to_json(v: bytes | str | None) -> Any:
    if v is None:
        return None

    s = to_str(v)
    if s:
        return json.loads(s)

    return None


def gen_key(ident: str) -> str:
    uuid = ident.split('/')[-1]
    return f'{REDIS_KEY_PREFIX}{uuid}'


class RedisReqMap(ReqMap):

    def __init__(self, redis: aioredis.Redis) -> None:
        self.redis = redis

    async def get(self, ident: str) -> Optional[Request]:
        key = gen_key(ident)
        data = to_json(await self.redis.get(key))
        if not data:
            return None

        req = Request(data['id'], data['parsers'])
        req.expired_at = data['expired_at']
        return req

    async def set(self, ident: str, req: Request) -> None:
        key = gen_key(ident)
        data = {
            'id': req.id,
            'parsers': req.parsers,
            'expired_at': req.expired_at,
        }

        await self.redis.setex(key, REDIS_REQ_TTL, json.dumps(data))

    async def pop(self, ident: str) -> Optional[Request]:
        req = await self.get(ident)
        key = gen_key(ident)
        await self.redis.delete(key)
        return req


@worker.func('process-bridged-mqtt-message')
async def process_bridged_mqtt_message(job: Any) -> Any:
    global req_map, mqtt
    message = json.loads(str(job.workload, 'utf-8'))
    topic = message['topic']

    payload = base64.b64decode(message['payload'])
    if req_map is None:
        redis = aioredis.from_url(
            redis_host,
            encoding='utf-8',
            decode_responses=True,
        )  # type: ignore[no-untyped-call]

        req_map = RedisReqMap(redis)

    if mqtt is None:
        mqtt = aiomqtt.Client(
            host,
            port,
            username=username,
            password=password,
        )
        logger.info('Try connect to mqtt')
        await mqtt.__aenter__()

    try:
        await process_mqtt_message(mqtt, req_map, topic, payload)
        return rsp.done()
    except Exception as exc:
        logger.exception(exc)
        mqtt = None
        return rsp.fail()


@worker.func('modbridge-edge-transform')
async def test_parser_transform(job: Any) -> Any:

    def done_error(message: str) -> Any:
        return rsp.done({
            'ok': False,
            'error': message,
        })

    try:
        message = json.loads(str(job.workload, 'utf-8'))
        parser = message.get('parser')
        if not isinstance(parser, dict):
            logger.error('invalid transform test input: parser is required')
            return done_error('invalid input: parser(dict) is required')

        raw_value = message.get('raw_value')
        data = message.get('data', {})
        if not isinstance(data, dict):
            data = {}

        transformed = await apply_transform(parser, raw_value)
        if transformed is None:
            return done_error(
                'transform failed: check parser.transform script and raw_value'
            )

        value = parse_transform_result(parser, transformed, data)
        return rsp.done({
            'ok': True,
            'transformed': transformed,
            'value': value,
            'data': data,
        })
    except Exception as exc:
        logger.exception(exc)
        return done_error(f'{type(exc).__name__}: {exc}')


async def main() -> None:
    await worker.connect(Transport(periodic_port))
    await worker.work(100)


if __name__ == '__main__':
    formatter = '[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} '
    formatter += '%(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    asyncio.run(main())
