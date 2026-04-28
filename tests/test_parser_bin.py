import asyncio
import unittest

from tests.test_utils import ensure_aiomqtt_stub, build_modbus_response

ensure_aiomqtt_stub()
from dtu import Request


class ParserBinTest(unittest.TestCase):

    def test_parse_bin16_parsers(self) -> None:
        req = Request('bin', [{
            'name': 'relay_bits',
            'unpack_func': 'bin16',
            'parsers': [{
                'index': 0,
                'name': 'bit0',
            }, {
                'index': 1,
                'name': 'bit1',
            }, {
                'index': 15,
                'name': 'bit15',
            }],
        }])
        # 0x8003 => bit15=1, bit1=1, bit0=1
        data = asyncio.run(req.parse(build_modbus_response('8003')))
        self.assertTrue(data['verified'])
        self.assertTrue(data['bit0'])
        self.assertTrue(data['bit1'])
        self.assertTrue(data['bit15'])


if __name__ == '__main__':
    unittest.main()
