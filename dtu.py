import asyncio
import aiomqtt
import json
import re
import crc
import textwrap
from config import host, port, username, password
from modbus_data_handler import ModbusDataHandler
from time import time
import logging
from typing import Any, Dict, Optional, List
from decimal import Decimal

import pydantic_monty

logger = logging.getLogger('dtu')

UNPACK_BYTE_SIZE = {
    '8': 1,
    '16': 2,
    '32': 4,
}
BIN_UNPACK_FUNCS = {'bin8', 'bin16'}
HEX_UNPACK_FUNCS = {'hex8', 'hex16', 'hex32'}
RE_SAFE_KEY_CHAR = re.compile('[a-zA-Z0-9_]')
RE_SPACE = re.compile('[ ]+')
TOPIC_REQUEST = 'request'
TOPIC_PONG = 'pong'
TOPIC_DTU_PUB = 'dtu/pub'
KEEPALIVE_PAYLOAD = b'www.usr.cn'


class InvalidRequest(Exception):
    pass


def safe_json(data: Any) -> Dict[str, Any]:
    try:
        if isinstance(data, (bytes, bytearray)):
            payload = str(data, 'utf-8', errors='ignore')
        else:
            payload = str(data)
        parsed = json.loads(payload)
        if isinstance(parsed, dict):
            return parsed
        return {}
    except Exception as exc:
        logger.error(data)
        logger.exception(exc)
        return {}


def isinrange(range_config: Any, value: float) -> bool:
    value = float(value)
    max_value = range_config.get('max')
    if max_value is not None:
        max_value = float(max_value)
        if value > max_value:
            return False

    min_value = range_config.get('min')
    if min_value is not None:
        min_value = float(min_value)
        if value < min_value:
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


def should_skip_by_filters(all_filters: Any, sensor_value: float) -> bool:
    if not isinstance(all_filters, list) or len(all_filters) == 0:
        return False

    filters = []
    neg_filters = []
    for filter_item in all_filters:
        typ = filter_item.get('type', 'range')
        if typ == 'range':
            filters.append(filter_item)
        if typ == 'range_ignore':
            neg_filters.append(filter_item)

    if len(filters) > 0:
        is_valid = False
        for filter_item in filters:
            if isinrange(filter_item, sensor_value):
                is_valid = True
                break
        if not is_valid:
            return True

    if len(neg_filters) > 0:
        is_valid = True
        for filter_item in neg_filters:
            if isinrange(filter_item, sensor_value):
                is_valid = False
                break
        if not is_valid:
            return True

    return False


def parse_transform_result(
    parser: Dict[str, Any],
    transformed: Any,
    data: Dict[str, Any],
) -> Optional[Any]:
    if not isinstance(transformed, dict):
        return transformed

    parser_name = str(parser.get('name'))
    transformed_value = None
    value_key = None
    value_keys = ['value', parser_name]
    for key in value_keys:
        if key in transformed:
            value_key = key
            transformed_value = transformed[key]
            break

    transformed_rest = {
        key: val for key, val in transformed.items() if key not in value_keys
    }
    if transformed_rest:
        data.update(transformed_rest)

    if value_key is None:
        return None

    return transformed_value


def read_parser_bytes(mdata: bytes, unpack_func: str) -> tuple[bytes, bytes]:
    for marker, size in UNPACK_BYTE_SIZE.items():
        if marker in unpack_func:
            return mdata[:size], mdata[size:]
    return b'', mdata


def split_ident_and_subtopic(topic: str) -> tuple[str, str]:
    idx = topic.find('/', 1)
    idx = topic.find('/', idx + 1)
    ident = topic[:idx]
    subtopic = topic[idx:]
    return ident, subtopic


def normalize_payload(payload: Any) -> Any:
    if isinstance(payload, (bytes, bytearray)):
        return payload.strip()
    return str(payload).strip()


def to_payload_bytes(payload: Any) -> bytes:
    if isinstance(payload, (bytes, bytearray)):
        return bytes(payload)
    return str(payload).encode('utf-8')


def is_json_bytes_payload(payload: Any) -> bool:
    return (
        isinstance(payload, (bytes, bytearray))
        and payload.startswith(b'{')
        and payload.endswith(b'}')
    )


async def send_ping(mqtt: Any, ident: str) -> None:
    await mqtt.publish(ident + '/ping')


def response_topic(ident: str, req_id: str) -> str:
    return ident + '/response/' + req_id


async def publish_json(mqtt: Any, topic: str, data: Dict[str, Any],
                       **kwargs: Any) -> None:
    await mqtt.publish(topic, payload=json.dumps(data), **kwargs)


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
    await publish_json(mqtt, response_topic(ident, req_id), data)


async def send_response_waiting(mqtt: Any, ident: str, req_id: str) -> None:
    data = {'modbus_state': 'waiting', 'err': 'MODBUS_STATE_WAITING'}
    await publish_json(mqtt, response_topic(ident, req_id), data)


class Request(object):

    def __init__(
        self,
        id: str,
        parsers: Optional[List[Dict[str, Any]]] = None,
    ) -> None:
        self.id: str = id
        self.parsers: List[Dict[str, Any]] = parsers or []
        self.expired_at: int = int(time()) + 10

    async def parse(self, payload: bytes) -> Dict[str, Any]:
        verified = crc.verify_modbus_crc(payload)
        payload_hex = payload.hex()
        data: Dict[str, Any] = {'modbus': payload_hex, 'verified': verified}

        if verified:
            mdata = payload[3:-2]
            for parser in self.parsers:
                if not mdata:
                    break

                unpack_func = parser['unpack_func']
                curr, mdata = read_parser_bytes(mdata, unpack_func)

                if not curr:
                    continue

                name = 'unpack_' + unpack_func
                unpack = getattr(ModbusDataHandler, name, None)
                if not unpack:
                    continue

                value = unpack(curr)

                if unpack_func in BIN_UNPACK_FUNCS:
                    if not isinstance(value, str):
                        continue

                    value_len = len(value)
                    bin_parsers = parser['parsers']
                    for bin_parser in bin_parsers:
                        idx = (-bin_parser['index'] - 1) % value_len
                        data[bin_parser['name']] = value[idx] == '1'
                    continue

                if unpack_func in HEX_UNPACK_FUNCS:
                    if isinstance(value, str):
                        data[parser['name']] = value
                    continue

                sensor_value = float(value)

                transform = parser.get('transform')
                if isinstance(transform, str) and transform.strip():
                    transformed = await apply_transform(parser, value)
                    if transformed is not None:
                        transformed_value = parse_transform_result(
                            parser, transformed, data)
                        if transformed_value is None:
                            continue
                        sensor_value = transformed_value

                scale = Decimal(str(parser.get('scale', 1)))
                offset = Decimal(str(parser.get('offset', 0)))
                sensor_decimal = Decimal(str(sensor_value)) * scale + offset
                sensor_value = float(sensor_decimal)

                decimal_places = get_valid_decimal_places(parser)
                if decimal_places is not None:
                    quant = Decimal(1).scaleb(-decimal_places)
                    sensor_value = float(
                        Decimal(str(sensor_value)).quantize(quant))

                all_filters = parser.get('filters', [])
                if should_skip_by_filters(all_filters, sensor_value):
                    continue

                data[parser['name']] = sensor_value

        return data


async def apply_transform(
    parser: Dict[str, Any],
    raw_value: Any,
) -> Optional[Any]:
    transform = parser.get('transform')
    if not isinstance(transform, str):
        return None

    expr = transform.strip()
    if not expr:
        return None

    script = textwrap.dedent(expr).strip()
    if not script:
        return None

    type_defs = (
        'raw_value: float = 0\n'
    )

    try:
        monty = pydantic_monty.Monty(
            script,
            inputs=['raw_value'],
            script_name='parser_transform.py',
            type_check=True,
            type_check_stubs=type_defs,
        )
        out = await monty.run_async(
            inputs={
                'raw_value': float(raw_value),
            },
        )
        return out
    except Exception as exc:
        logger.exception(exc)
        logger.warning('transform failed, parser=%s', parser.get('name', ''))
        return None


class ReqMap(object):

    async def get(self, ident: str) -> Optional[Request]:
        raise NotImplementedError("Not implement")

    async def set(self, ident: str, req: Request) -> None:
        raise NotImplementedError("Not implement")

    async def pop(self, ident: str) -> Optional[Request]:
        raise NotImplementedError("Not implement")


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
    data = await req.parse(payload)
    await publish_json(mqtt, response_topic(ident, req.id), data)


def normal_key(key: str) -> str:
    out = ''
    for c in key:
        if RE_SAFE_KEY_CHAR.search(c):
            out += c
            continue

        out += ' '

    return RE_SPACE.sub('_', out)


async def forward_dtu(mqtt: Any, ident: str, data: Dict[str, Any]) -> None:
    state = data.pop('state', None)
    params = data.get('params')

    if state:
        await publish_json(
            mqtt,
            ident + '/attributes',
            {'state': state},
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
    await publish_json(mqtt, ident + '/telemetry', out)


async def process_mqtt_message(mqtt: Any, req_map: ReqMap, topic: str,
                               payload: Any) -> None:
    ident, topic = split_ident_and_subtopic(topic)
    logger.debug('Device: ' + ident)
    payload = normalize_payload(payload)

    if TOPIC_REQUEST in topic:
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

    if TOPIC_PONG in topic:
        return

    if TOPIC_DTU_PUB in topic:
        if is_json_bytes_payload(payload):
            await forward_dtu(mqtt, ident, safe_json(payload))
        else:
            raw_payload = to_payload_bytes(payload)

            if raw_payload == KEEPALIVE_PAYLOAD:
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
