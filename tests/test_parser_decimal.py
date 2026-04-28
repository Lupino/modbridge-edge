import unittest
import asyncio

from tests.test_utils import ensure_aiomqtt_stub, build_modbus_response

ensure_aiomqtt_stub()
from dtu import Request


class ParserDecimalTest(unittest.TestCase):

    def test_apply_decimal_places_int(self) -> None:
        req = Request('r1', [{
            'name': 'temperature',
            'unpack_func': 'uint16_AB',
            'scale': 0.1,
            'offset': 0,
            'decimal_places': 1,
        }])
        data = asyncio.run(req.parse(build_modbus_response('0001')))
        self.assertTrue(data['verified'])
        self.assertEqual(data['temperature'], 0.1)

    def test_apply_decimal_places_string(self) -> None:
        req = Request('r2', [{
            'name': 'humidity',
            'unpack_func': 'uint16_AB',
            'scale': 0.01,
            'offset': 0,
            'decimal_places': '2',
        }])
        data = asyncio.run(req.parse(build_modbus_response('0001')))
        self.assertEqual(data['humidity'], 0.01)

    def test_apply_decimal_point_alias(self) -> None:
        req = Request('r3', [{
            'name': 'pressure',
            'unpack_func': 'uint16_AB',
            'scale': 1,
            'offset': 0.12345,
            'decimal_point': 2,
        }])
        data = asyncio.run(req.parse(build_modbus_response('0001')))
        self.assertEqual(data['pressure'], 1.12)

    def test_invalid_decimal_config_is_ignored(self) -> None:
        req = Request('r4', [{
            'name': 'value_neg',
            'unpack_func': 'uint16_AB',
            'scale': 0.1,
            'offset': 0,
            'decimal_places': -1,
        }, {
            'name': 'value_str',
            'unpack_func': 'uint16_AB',
            'scale': 0.1,
            'offset': 0,
            'decimal_places': '1.5',
        }])
        data = asyncio.run(req.parse(build_modbus_response('00010001')))
        self.assertEqual(data['value_neg'], 0.1)
        self.assertEqual(data['value_str'], 0.1)

    def test_apply_transform(self) -> None:
        req = Request('r5', [{
            'name': 'temperature_c',
            'unpack_func': 'uint16_AB',
            'scale': 0.1,
            'offset': 0,
            'transform': 'raw_value + 1',
        }])
        data = asyncio.run(req.parse(build_modbus_response('000A')))
        self.assertEqual(data['temperature_c'], 1.1)

    def test_transform_failure_fallback(self) -> None:
        req = Request('r6', [{
            'name': 'temperature',
            'unpack_func': 'uint16_AB',
            'scale': 0.1,
            'offset': 0,
            'transform': 'raw_value + unknown_symbol',
        }])
        data = asyncio.run(req.parse(build_modbus_response('000A')))
        self.assertEqual(data['temperature'], 1.0)

    def test_apply_multiline_transform(self) -> None:
        req = Request('r7', [{
            'name': 'encoded_decimal',
            'unpack_func': 'uint16_AB',
            'scale': 1,
            'offset': 0,
            'transform': '\n'.join([
                'digits = str(int(raw_value))',
                'decimal_places = int(digits[0])',
                'mantissa = int(digits[1:])',
                'mantissa / (10 ** decimal_places)',
            ]),
        }])
        data = asyncio.run(req.parse(build_modbus_response('52D2')))
        self.assertEqual(data['encoded_decimal'], 12.02)

    def test_transform_before_decimal_places(self) -> None:
        req = Request('r8', [{
            'name': 'value',
            'unpack_func': 'uint16_AB',
            'scale': 0.01,
            'offset': 0,
            'transform': 'raw_value / 3',
            'decimal_places': 2,
        }])
        data = asyncio.run(req.parse(build_modbus_response('0001')))
        self.assertEqual(data['value'], 0.0)

    def test_transform_dict_updates_data(self) -> None:
        req = Request('r9', [{
            'name': 'temperature_c',
            'unpack_func': 'uint16_AB',
            'scale': 0.1,
            'offset': 0,
            'transform': '{"value": raw_value + 1, "extra": 123}',
        }])
        data = asyncio.run(req.parse(build_modbus_response('000A')))
        self.assertEqual(data['temperature_c'], 1.1)
        self.assertEqual(data['extra'], 123)

    def test_transform_dict_parser_name_runs_normal_flow(self) -> None:
        req = Request('r10', [{
            'name': 'temperature_c',
            'unpack_func': 'uint16_AB',
            'scale': 0.1,
            'offset': 0,
            'transform': '{"temperature_c": raw_value + 1, "extra": 123}',
        }])
        data = asyncio.run(req.parse(build_modbus_response('000A')))
        self.assertEqual(data['temperature_c'], 1.1)
        self.assertEqual(data['extra'], 123)


if __name__ == '__main__':
    unittest.main()
