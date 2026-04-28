import sys
import types

import crc


def ensure_aiomqtt_stub() -> None:
    if 'aiomqtt' not in sys.modules:
        sys.modules['aiomqtt'] = types.ModuleType('aiomqtt')


def build_modbus_response(data_hex: str) -> bytes:
    body = bytes.fromhex(data_hex)
    payload = bytes([0x01, 0x03, len(body)]) + body
    return crc.add_modbus_crc(payload)
