import asyncio
from redis import asyncio as aioredis
from aio_periodic import Transport, Worker
import aiomqtt
import base64
from config import host, port, username, password, periodic_port, redis_host
import json

from typing import Any, Optional, cast

from dtu import process_mqtt_message, ReqMap, Request
import logging

worker = Worker()
req_map: Optional[ReqMap] = None
mqtt: Optional[aiomqtt.Client] = None


def to_str(v: bytes | str) -> str:
    if isinstance(v, bytes):
        return str(v, 'utf-8')
    else:
        return v


def to_json(v: bytes) -> Any:
    s = to_str(v)
    if s:
        return json.loads(s)

    return v


def from_json(v: Any) -> str:
    return json.dumps(v)


def gen_key(ident: str) -> str:
    uuid = ident.split('/')[-1]
    return f'esp32-dtu-bridge:{uuid}'


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

        await self.redis.setex(key, 20, json.dumps(data))

    async def pop(self, ident: str) -> Optional[Request]:
        req = await self.get(ident)
        key = gen_key(ident)
        await self.redis.delete(key)
        return req


@worker.func('process-dtu-bridge-message')
async def process_dtu_bridge_message(job: Any) -> None:
    message = json.loads(str(job.workload, 'utf-8'))
    topic = message['topic']

    payload = base64.b64decode(message['payload'])
    if mqtt is None or req_map is None:
        raise RuntimeError('mqtt or req_map not initialized')

    await process_mqtt_message(mqtt, req_map, topic, payload)


async def main() -> None:
    global req_map, mqtt
    redis = aioredis.from_url(
        redis_host,
        encoding='utf-8',
        decode_responses=True,
    )  # type: ignore[no-untyped-call]
    req_map = RedisReqMap(redis)

    async with aiomqtt.Client(
            host,
            port,
            username=username,
            password=password,
    ) as mqttClient:
        mqtt = mqttClient
        await worker.connect(Transport(periodic_port))
        await worker.work(100)


if __name__ == '__main__':
    formatter = '[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} '
    formatter += '%(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    asyncio.run(main())
