import asyncio
import aiomqtt
import json
import re
import crc
from config import host, port, username, password
from modbus_data_handler import ModbusDataHandler
from time import time
import logging
from typing import Any, Dict, Optional, List
from decimal import Decimal

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


def isinrange(range: Any, value: float) -> bool:
    value = float(value)
    max = range.get('max')
    if max is not None:
        max = float(max)
        if value > max:
            return False

    min = range.get('min')
    if min is not None:
        min = float(min)
        if value < min:
            return False

    return True


def get_valid_decimal_places(parser: Dict[str, Any]) -> Optional[int]:
    """
    Return a validated decimal places value from parser config.

    Supported keys (priority order):
    - decimal_places
    - decimal_point

    Valid values:
    - int >= 0
    - numeric string like "2"
    - float/Decimal representing an integer (e.g. 2.0)
    """
    raw = parser.get('decimal_places', parser.get('decimal_point'))
    if raw is None or isinstance(raw, bool):
        return None

    places: Optional[int] = None
    if isinstance(raw, int):
        places = raw
    elif isinstance(raw, str):
        s = raw.strip()
        if s.isdigit():
            places = int(s)
    elif isinstance(raw, float):
        if raw.is_integer():
            places = int(raw)
    elif isinstance(raw, Decimal):
        if raw == raw.to_integral_value():
            places = int(raw)

    if places is None or places < 0:
        return None

    # Keep quantize predictable and avoid unreasonable precision config.
    if places > 18:
        return None

    return places


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
            pack_func = data.get('pack_func', 'uint16_AB')
            pack = getattr(ModbusDataHandler, 'pack_' + pack_func, None)
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
    data = {'modbus_state': 'waiting', 'err': 'MODBUS_STATE_WAITING'}
    await mqtt.publish(ident + '/response/' + req_id, payload=json.dumps(data))


class Request(object):

    def __init__(
        self,
        id: str,
        parsers: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
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

                if '8' in unpack_func:
                    curr = mdata[:1]
                    mdata = mdata[1:]

                if '16' in unpack_func:
                    curr = mdata[:2]
                    mdata = mdata[2:]

                if '32' in unpack_func:
                    curr = mdata[:4]
                    mdata = mdata[4:]

                if not curr:
                    continue

                name = 'unpack_' + unpack_func
                unpack = getattr(ModbusDataHandler, name, None)
                if not unpack:
                    continue

                value = unpack(curr)

                if unpack_func in ['bin8', 'bin16']:
                    if not isinstance(value, str):
                        continue

                    value_len = len(value)
                    bin_parsers = parser['parsers']
                    for bin_parser in bin_parsers:
                        idx = (-bin_parser['index'] - 1) % value_len
                        data[bin_parser['name']] = value[idx] == '1'
                    continue

                scale = Decimal(str(parser.get('scale', 1)))
                offset = Decimal(str(parser.get('offset', 0)))

                sensor_decimal = Decimal(str(value)) * scale + offset

                decimal_places = get_valid_decimal_places(parser)
                if decimal_places is not None:
                    quant = Decimal(1).scaleb(-decimal_places)
                    sensor_decimal = sensor_decimal.quantize(quant)

                sensor_value = float(sensor_decimal)

                all_filters = parser.get('filters', [])

                if isinstance(all_filters, list) and len(all_filters) > 0:
                    filters = []
                    neg_filters = []
                    for filter in all_filters:
                        typ = filter.get('type', 'range')
                        if typ == 'range':
                            filters.append(filter)

                        if typ == 'range_ignore':
                            neg_filters.append(filter)

                    if len(filters) > 0:
                        is_valid = False

                        for filter in filters:
                            if isinrange(filter, sensor_value):
                                is_valid = True
                                break

                        if not is_valid:
                            continue

                    if len(neg_filters) > 0:
                        is_valid = True

                        for filter in neg_filters:
                            if isinrange(filter, sensor_value):
                                is_valid = False
                                break

                        if not is_valid:
                            continue

                data[parser['name']] = sensor_value

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
    state = data.pop('state', None)
    params = data.get('params')

    if state:
        await mqtt.publish(
            ident + '/attributes',
            payload=json.dumps({'state': state}),
            retain=True,
            qos=1,
        )
        return

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
            if isinstance(payload, (bytes, bytearray)):
                raw_payload = payload
            else:
                raw_payload = str(payload).encode('utf-8')

            if raw_payload == b'www.usr.cn':
                return

            popped = await req_map.pop(ident)
            if popped is not None:
                req = popped
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
