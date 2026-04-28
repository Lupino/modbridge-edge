import unittest

import crc


class CrcTest(unittest.TestCase):

    def test_crc_matches_known_vector(self) -> None:
        data = bytes.fromhex('010300000001')
        value = crc.modbus_crc16_table(data)
        self.assertEqual(value, 0x0A84)

    def test_table_and_simple_are_consistent(self) -> None:
        data = bytes.fromhex('01050000FF00')
        self.assertEqual(
            crc.modbus_crc16_table(data),
            crc.modbus_crc16_simple(data),
        )

    def test_add_and_verify_crc(self) -> None:
        data = bytes.fromhex('010300000001')
        packet = crc.add_modbus_crc(data)
        self.assertTrue(crc.verify_modbus_crc(packet))
        self.assertFalse(crc.verify_modbus_crc(packet[:-1] + b'\x00'))


if __name__ == '__main__':
    unittest.main()
