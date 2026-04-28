import asyncio
import unittest

from tests.test_utils import ensure_aiomqtt_stub, build_modbus_response

ensure_aiomqtt_stub()
from dtu import Request


class ParserHexTest(unittest.TestCase):

    def test_parse_hex8_hex16_hex32(self) -> None:
        req = Request('hex', [{
            'name': 'hex8_v',
            'unpack_func': 'hex8',
        }, {
            'name': 'hex16_v',
            'unpack_func': 'hex16',
        }, {
            'name': 'hex32_v',
            'unpack_func': 'hex32',
        }])
        data = asyncio.run(req.parse(build_modbus_response('AB12CD00112233')))

        self.assertTrue(data['verified'])
        self.assertEqual(data['hex8_v'], 'ab')
        self.assertEqual(data['hex16_v'], '12cd')
        self.assertEqual(data['hex32_v'], '00112233')


if __name__ == '__main__':
    unittest.main()
