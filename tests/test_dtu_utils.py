import sys
import types
import unittest

if 'aiomqtt' not in sys.modules:
    sys.modules['aiomqtt'] = types.ModuleType('aiomqtt')

from dtu import (get_valid_decimal_places, is_json_bytes_payload, isinrange,
                 normal_key, normalize_payload, parse_transform_result,
                 read_parser_bytes,
                 should_skip_by_filters,
                 split_ident_and_subtopic, to_payload_bytes)


class DtuUtilsTest(unittest.TestCase):

    def test_normal_key_replaces_non_word_chars(self) -> None:
        self.assertEqual(normal_key('A B-C.D'), 'A_B_C_D')

    def test_read_parser_bytes_by_unpack_func(self) -> None:
        curr, rest = read_parser_bytes(b'\x01\x02\x03', 'uint16_AB')
        self.assertEqual(curr, b'\x01\x02')
        self.assertEqual(rest, b'\x03')

        curr8, rest8 = read_parser_bytes(b'\x01\x02', 'uint8')
        self.assertEqual(curr8, b'\x01')
        self.assertEqual(rest8, b'\x02')

        curr_unknown, rest_unknown = read_parser_bytes(b'\x01', 'custom')
        self.assertEqual(curr_unknown, b'')
        self.assertEqual(rest_unknown, b'\x01')

    def test_split_ident_and_subtopic(self) -> None:
        ident, subtopic = split_ident_and_subtopic('/p/u/request/r1')
        self.assertEqual(ident, '/p/u')
        self.assertEqual(subtopic, '/request/r1')

    def test_normalize_payload(self) -> None:
        self.assertEqual(normalize_payload(b' abc \n'), b'abc')
        self.assertEqual(normalize_payload(' abc \n'), 'abc')
        self.assertEqual(normalize_payload(123), '123')

    def test_to_payload_bytes(self) -> None:
        self.assertEqual(to_payload_bytes(b'abc'), b'abc')
        self.assertEqual(to_payload_bytes(bytearray(b'abc')), b'abc')
        self.assertEqual(to_payload_bytes('abc'), b'abc')
        self.assertEqual(to_payload_bytes(123), b'123')

    def test_is_json_bytes_payload(self) -> None:
        self.assertTrue(is_json_bytes_payload(b'{}'))
        self.assertTrue(is_json_bytes_payload(bytearray(b'{"k":1}')))
        self.assertFalse(is_json_bytes_payload(b'[]'))
        self.assertFalse(is_json_bytes_payload('{"k":1}'))

    def test_get_valid_decimal_places(self) -> None:
        self.assertEqual(get_valid_decimal_places({'decimal_places': 2}), 2)
        self.assertEqual(get_valid_decimal_places({'decimal_point': '3'}), 3)
        self.assertIsNone(get_valid_decimal_places({'decimal_places': -1}))
        self.assertIsNone(get_valid_decimal_places({'decimal_places': '1.5'}))
        self.assertIsNone(get_valid_decimal_places({'decimal_places': True}))

    def test_should_skip_by_filters(self) -> None:
        allow = [{'type': 'range', 'min': 0, 'max': 10}]
        deny = [{'type': 'range_ignore', 'min': 5, 'max': 6}]

        self.assertFalse(should_skip_by_filters(allow, 3))
        self.assertTrue(should_skip_by_filters(allow, 11))
        self.assertTrue(should_skip_by_filters(deny, 5.5))
        self.assertFalse(should_skip_by_filters(deny, 7))

    def test_isinrange(self) -> None:
        self.assertTrue(isinrange({'min': 0, 'max': 10}, 5))
        self.assertTrue(isinrange({'min': 0, 'max': 10}, 0))
        self.assertTrue(isinrange({'min': 0, 'max': 10}, 10))
        self.assertFalse(isinrange({'min': 0, 'max': 10}, -0.1))
        self.assertFalse(isinrange({'min': 0, 'max': 10}, 10.1))

    def test_parse_transform_result(self) -> None:
        parser = {'name': 'temperature_c'}

        data_a = {'modbus': 'xx'}
        out_a = parse_transform_result(
            parser,
            {'value': 1.1, 'extra': 123},
            data_a,
        )
        self.assertEqual(out_a, 1.1)
        self.assertEqual(data_a['extra'], 123)

        data_b = {'modbus': 'xx'}
        out_b = parse_transform_result(
            parser,
            {'temperature_c': 2.2, 'unit': 'C'},
            data_b,
        )
        self.assertEqual(out_b, 2.2)
        self.assertEqual(data_b['unit'], 'C')

        data_c = {'modbus': 'xx'}
        out_c = parse_transform_result(
            parser,
            {'aux': 999},
            data_c,
        )
        self.assertIsNone(out_c)
        self.assertEqual(data_c['aux'], 999)


if __name__ == '__main__':
    unittest.main()
