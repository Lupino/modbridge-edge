import unittest

from modbus_data_handler import ModbusDataHandler


class ModbusDataHandlerTest(unittest.TestCase):

    def test_bcd16_roundtrip(self) -> None:
        value = 1234
        packed = ModbusDataHandler.pack_bcd16(value)
        self.assertEqual(packed, bytes.fromhex('1234'))
        self.assertEqual(ModbusDataHandler.unpack_bcd16(packed), value)

    def test_bcd32_roundtrip(self) -> None:
        value = 12345678
        packed = ModbusDataHandler.pack_bcd32(value)
        self.assertEqual(packed, bytes.fromhex('12345678'))
        self.assertEqual(ModbusDataHandler.unpack_bcd32(packed), value)

    def test_bcd16_invalid_input_range(self) -> None:
        with self.assertRaises(ValueError):
            ModbusDataHandler.pack_bcd16(10000)

    def test_bcd32_invalid_input_range(self) -> None:
        with self.assertRaises(ValueError):
            ModbusDataHandler.pack_bcd32(100000000)

    def test_unpack_invalid_bcd_data(self) -> None:
        with self.assertRaises(ValueError):
            ModbusDataHandler.unpack_bcd16(bytes.fromhex('1A34'))

        with self.assertRaises(ValueError):
            ModbusDataHandler.unpack_bcd32(bytes.fromhex('1234EF78'))


if __name__ == '__main__':
    unittest.main()
