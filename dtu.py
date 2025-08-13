import asyncio
import aiomqtt
import json
import re
import crc
from config import host, port, username, password
import pack
import logging

from collections import defaultdict
'''
{
  "method": "modbus_req",
  "modbus": "0106000f0001",
  "crc": true
}

{
  "method": "modbus_req",
  "addr": "01",
  "op": "06",
  "reg": "000F",
  "data": "0001",
  "crc": true
}

{
  "method": "modbus_req",
  "addr": "01",
  "op": "06",
  "reg": "000F",
  "data": 1,
  "pack_func": "uint16_AB",
  "crc": true
}
'''

logger = logging.getLogger('dtu')


class InvalidRequest(Exception):
    pass


class IgnoreRequest(Exception):
    pass


def safe_json(data):
    try:
        return json.loads(str(data, 'utf-8', errors='ignore'))
    except Exception as exc:
        logger.error(data)
        logger.exception(exc)


async def send_ping(mqtt, ident):
    await mqtt.publish(ident + '/ping')


async def forward_request(mqtt, ident, data):

    def print_error():
        logger.error('Error: invalid request' + str(data))
        raise InvalidRequest()

    if data.get('method') != 'modbus_req':
        raise IgnoreRequest()

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
            format = data.get('pack_func', 'uint16_AB')
            v = pack.pack_value(pl, format)

            if not v:
                return print_error()

            modbus += v.hex()

    if not modbus:
        return print_error()

    modbus_bytes = bytes.fromhex(modbus)
    if data.get('crc'):
        modbus_bytes = crc.add_modbus_crc(modbus_bytes)

    await mqtt.publish(ident + '/dtu/sub', payload=modbus_bytes)


async def send_response_error(mqtt, ident, req_id):
    data = {'err': 'Invalid request'}
    await mqtt.publish(ident + '/response/' + req_id, payload=json.dumps(data))


async def forward_response(mqtt, ident, req_id, payload):
    data = {'modbus': payload.hex()}
    await mqtt.publish(ident + '/response/' + req_id, payload=json.dumps(data))


def normal_key(key):
    re_chr = re.compile('[a-zA-Z0-9_]')
    re_space = re.compile('[ ]+')

    out = ''
    for c in key:
        if re_chr.search(c):
            out += c
            continue

        out += ' '

    return re_space.sub('_', out)


async def forward_dtu(mqtt, ident, data):
    params = data.get('params')

    if not params:
        await send_ping(mqtt, ident)
        return

    out = {}
    for k, v in params.items():
        out[normal_key(k)] = v
    await mqtt.publish(ident + '/telemetry', payload=json.dumps(out))


async def main():
    async with aiomqtt.Client(host, port, username=username,
                              password=password) as mqtt:
        await mqtt.subscribe('/+/+/dtu/#')
        await mqtt.subscribe('/+/+/request/#')
        # await mqtt.subscribe('/+/+/pong/#')
        # await send_ping(mqtt)
        req_map = defaultdict(list)
        async for message in mqtt.messages:
            topic = str(message.topic)
            idx = topic.find('/', 1)
            idx = topic.find('/', idx + 1)
            ident = topic[:idx]
            logger.debug('Device: ' + ident)
            topic = topic[idx:]

            payload = message.payload.strip()

            logger.debug(f'{topic}:{payload}')

            if topic.find('request') > -1:
                req_id = topic.split('/')[-1]
                try:
                    await forward_request(mqtt, ident, safe_json(payload))
                    req_map[ident].append(req_id)
                except InvalidRequest:
                    send_response_error(mqtt, ident, req_id)
                except IgnoreRequest:
                    pass
                continue

            if topic.find('pong') > -1:
                continue

            if topic.find('dtu/pub') > -1:
                if payload.startswith(b'{') and payload.endswith(b'}'):
                    await forward_dtu(mqtt, ident, safe_json(payload))
                else:
                    req_ids = req_map.pop(ident, [])
                    for req_id in req_ids:
                        await forward_response(mqtt, ident, req_id, payload)

                continue

formatter = '[%(asctime)s] %(name)s {%(filename)s:%(lineno)d} %(levelname)s'
formatter += ' - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=formatter)
asyncio.run(main())
