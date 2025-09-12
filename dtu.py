import asyncio
import aiomqtt
import json
import re
import crc
from config import host, port, username, password
from modbus_data_handler import ModbusDataHandler
from time import time
import logging
from typing import Any, Dict, Optional, Callable, List

logger = logging.getLogger('dtu')


class InvalidRequest(Exception):
    pass


def safe_json(data: Any) -> Dict[str, Any]:
    try:
        parsed = json.loads(str(data, 'utf-8', errors='ignore'))
        if isinstance(parsed, dict):
            return parsed
        return {}
    except Exception as exc:
        logger.error(data)
        logger.exception(exc)
        return {}


async def send_ping(mqtt: Any, ident: str) -> None:
    await mqtt.publish(ident + '/ping')


async def forward_request(mqtt: Any, ident: str, data: Dict[str, Any]) -> None:

    def print_error() -> None:
        logger.error('Error: invalid request' + str(data))
        raise InvalidRequest()

    modbus = data.get('modbus', '')
    if not modbus:
        for k in ['addr', 'op', 'reg']:
            v = data.get(k)
            if not v:
                return print_error()

            modbus += data[k]

        pl = data.get('data')
        if pl is None:
            return print_error()

        if isinstance(pl, str):
            modbus += pl
        elif isinstance(pl, int):
            fmt = data.get('pack_func', 'uint16_AB')
            pack: Optional[Callable[[int],
                                    bytes]] = getattr(ModbusDataHandler,
                                                      'pack_' + fmt, None)
            if not pack:
                return print_error()

            v = pack(pl)

            modbus += v.hex()

    if not modbus:
        return print_error()

    modbus_bytes = bytes.fromhex(modbus)
    if data.get('crc'):
        modbus_bytes = crc.add_modbus_crc(modbus_bytes)

    await mqtt.publish(ident + '/dtu/sub', payload=modbus_bytes)


async def send_response_error(mqtt: Any, ident: str, req_id: str) -> None:
    data = {'err': 'Invalid request'}
    await mqtt.publish(ident + '/response/' + req_id, payload=json.dumps(data))


async def send_response_waiting(mqtt: Any, ident: str, req_id: str) -> None:
    data = {'modbus_state': 'waiting'}
    await mqtt.publish(ident + '/response/' + req_id, payload=json.dumps(data))


class Request(object):

    def __init__(self,
                 id: str,
                 parsers: Optional[List[Dict[str, Any]]] = None) -> None:
        self.id: str = id
        self.parsers: List[Dict[str, Any]] = parsers or []
        self.expired_at: int = int(time()) + 10

    def parse(self, payload: bytes) -> Dict[str, Any]:
        verified = crc.verify_modbus_crc(payload)
        payload_hex = payload.hex()
        data: Dict[str, Any] = {'modbus': payload_hex, 'verified': verified}

        if verified:
            mdata = payload[3:-2]
            for parser in self.parsers:
                if not mdata:
                    break

                unpack_func = parser['unpack_func']
                curr = b''
                if '16' in unpack_func:
                    curr = mdata[:2]
                    mdata = mdata[2:]

                if '32' in unpack_func:
                    curr = mdata[:4]
                    mdata = mdata[4:]

                if not curr:
                    continue

                unpack = getattr(ModbusDataHandler, 'unpack_' + unpack_func,
                                 None)
                if not unpack:
                    continue

                value = unpack(curr)

                scale = parser.get('scale', 1)
                offset = parser.get('offset', 0)

                data[parser['name']] = value * scale + offset

        return data


class ReqMap(object):

    async def get(self, ident: str) -> Optional[Request]:
        raise Exception("Not implement")

    async def set(self, ident: str, req: Request) -> None:
        raise Exception("Not implement")

    async def pop(self, ident: str) -> Optional[Request]:
        raise Exception("Not implement")


class DictReqMap(ReqMap):

    def __init__(self) -> None:
        self.data: Dict[str, Request] = {}

    async def get(self, ident: str) -> Optional[Request]:
        return self.data.get(ident)

    async def set(self, ident: str, req: Request) -> None:
        self.data[ident] = req

    async def pop(self, ident: str) -> Optional[Request]:
        return self.data.pop(ident, None)


async def forward_response(mqtt: Any, ident: str, req: Request,
                           payload: bytes) -> None:
    data = req.parse(payload)
    await mqtt.publish(ident + '/response/' + req.id, payload=json.dumps(data))


def normal_key(key: str) -> str:
    re_chr = re.compile('[a-zA-Z0-9_]')
    re_space = re.compile('[ ]+')

    out = ''
    for c in key:
        if re_chr.search(c):
            out += c
            continue

        out += ' '

    return re_space.sub('_', out)


async def forward_dtu(mqtt: Any, ident: str, data: Dict[str, Any]) -> None:
    params = data.get('params')

    if not params:
        await send_ping(mqtt, ident)
        return

    out: Dict[str, Any] = {}
    for k, v in params.items():
        out[normal_key(k)] = v
    await mqtt.publish(ident + '/telemetry', payload=json.dumps(out))


async def process_mqtt_message(mqtt: Any, req_map: ReqMap, topic: str,
                               payload: Any) -> None:
    idx = topic.find('/', 1)
    idx = topic.find('/', idx + 1)
    ident = topic[:idx]
    logger.debug('Device: ' + ident)
    topic = topic[idx:]

    if isinstance(payload, (bytes, bytearray)):
        payload = payload.strip()
    else:
        payload = str(payload).strip()

    if topic.find('request') > -1:
        req_id = topic.split('/')[-1]
        data = safe_json(payload)
        method = data.get('method')
        if method != 'modbus_req':
            return

        prev_req = await req_map.get(ident)
        now = int(time())
        if prev_req and prev_req.expired_at > now:
            await send_response_waiting(mqtt, ident, req_id)
            return

        req = Request(req_id, data.get('parsers', []))

        try:
            await forward_request(mqtt, ident, data)
            await req_map.set(ident, req)
        except InvalidRequest:
            await send_response_error(mqtt, ident, req_id)

        return

    if topic.find('pong') > -1:
        return

    if topic.find('dtu/pub') > -1:
        if isinstance(payload, (bytes, bytearray)) and payload.startswith(
                b'{') and payload.endswith(b'}'):
            await forward_dtu(mqtt, ident, safe_json(payload))
        else:
            popped = await req_map.pop(ident)
            if popped is not None:
                req = popped
                if isinstance(payload, (bytes, bytearray)):
                    raw_payload = payload
                else:
                    raw_payload = str(payload).encode('utf-8')
                await forward_response(mqtt, ident, req, raw_payload)
        return


async def main() -> None:
    async with aiomqtt.Client(
            host,
            port,
            username=username,
            password=password,
    ) as mqtt:
        await mqtt.subscribe('/+/+/dtu/#')
        await mqtt.subscribe('/+/+/request/#')
        req_map = DictReqMap()
        async for message in mqtt.messages:
            topic = str(message.topic)
            payload = message.payload
            await process_mqtt_message(mqtt, req_map, topic, payload)


if __name__ == '__main__':
    formatter = '[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} '
    formatter += '%(levelname)s - %(message)s'
    logging.basicConfig(level=logging.DEBUG, format=formatter)
    asyncio.run(main())
