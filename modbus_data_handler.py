import struct


class ModbusDataHandler:
    """
    Handle packing and unpacking of Modbus binary data with
    various data types and byte orders.

    Byte order notation:
    - AB: Big-endian (most significant byte first)
    - BA: Little-endian (least significant byte first)
    - ABCD: Big-endian 32-bit
    - DCBA: Little-endian 32-bit
    - CDAB: Middle-endian (big-endian words, little-endian bytes)
    - BADC: Middle-endian (little-endian words, big-endian bytes)
    """

    @staticmethod
    def pack_uint16_AB(value):
        """Pack unsigned 16-bit integer, big-endian"""
        return struct.pack('>H', value)

    @staticmethod
    def unpack_uint16_AB(data):
        """Unpack unsigned 16-bit integer, big-endian"""
        return struct.unpack('>H', data)[0]

    @staticmethod
    def pack_uint16_BA(value):
        """Pack unsigned 16-bit integer, little-endian"""
        return struct.pack('<H', value)

    @staticmethod
    def unpack_uint16_BA(data):
        """Unpack unsigned 16-bit integer, little-endian"""
        return struct.unpack('<H', data)[0]

    @staticmethod
    def pack_uint32_ABCD(value):
        """Pack unsigned 32-bit integer, big-endian"""
        return struct.pack('>I', value)

    @staticmethod
    def unpack_uint32_ABCD(data):
        """Unpack unsigned 32-bit integer, big-endian"""
        return struct.unpack('>I', data)[0]

    @staticmethod
    def pack_uint32_DCBA(value):
        """Pack unsigned 32-bit integer, little-endian"""
        return struct.pack('<I', value)

    @staticmethod
    def unpack_uint32_DCBA(data):
        """Unpack unsigned 32-bit integer, little-endian"""
        return struct.unpack('<I', data)[0]

    @staticmethod
    def pack_uint32_CDAB(value):
        """Pack unsigned 32-bit integer, middle-endian (swap 16-bit words)"""
        # Convert to big-endian first, then swap the 16-bit words
        big_endian = struct.pack('>I', value)
        return big_endian[2:4] + big_endian[0:2]

    @staticmethod
    def unpack_uint32_CDAB(data):
        """Unpack unsigned 32-bit integer, middle-endian (swap 16-bit words)"""
        # Swap the 16-bit words, then unpack as big-endian
        swapped = data[2:4] + data[0:2]
        return struct.unpack('>I', swapped)[0]

    @staticmethod
    def pack_uint32_BADC(value):
        """Pack unsigned 32-bit integer,
        middle-endian (swap bytes within words)"""
        # Convert to big-endian, then swap bytes within each 16-bit word
        big_endian = struct.pack('>I', value)
        return big_endian[1:2] + big_endian[0:1] + big_endian[
            3:4] + big_endian[2:3]

    @staticmethod
    def unpack_uint32_BADC(data):
        """Unpack unsigned 32-bit integer,
        middle-endian (swap bytes within words)"""
        # Swap bytes within each 16-bit word, then unpack as big-endian
        swapped = data[1:2] + data[0:1] + data[3:4] + data[2:3]
        return struct.unpack('>I', swapped)[0]

    @staticmethod
    def pack_int16_AB(value):
        """Pack signed 16-bit integer, big-endian"""
        return struct.pack('>h', value)

    @staticmethod
    def unpack_int16_AB(data):
        """Unpack signed 16-bit integer, big-endian"""
        return struct.unpack('>h', data)[0]

    @staticmethod
    def pack_int16_BA(value):
        """Pack signed 16-bit integer, little-endian"""
        return struct.pack('<h', value)

    @staticmethod
    def unpack_int16_BA(data):
        """Unpack signed 16-bit integer, little-endian"""
        return struct.unpack('<h', data)[0]

    @staticmethod
    def pack_int32_ABCD(value):
        """Pack signed 32-bit integer, big-endian"""
        return struct.pack('>i', value)

    @staticmethod
    def unpack_int32_ABCD(data):
        """Unpack signed 32-bit integer, big-endian"""
        return struct.unpack('>i', data)[0]

    @staticmethod
    def pack_int32_DCBA(value):
        """Pack signed 32-bit integer, little-endian"""
        return struct.pack('<i', value)

    @staticmethod
    def unpack_int32_DCBA(data):
        """Unpack signed 32-bit integer, little-endian"""
        return struct.unpack('<i', data)[0]

    @staticmethod
    def pack_int32_CDAB(value):
        """Pack signed 32-bit integer, middle-endian (swap 16-bit words)"""
        big_endian = struct.pack('>i', value)
        return big_endian[2:4] + big_endian[0:2]

    @staticmethod
    def unpack_int32_CDAB(data):
        """Unpack signed 32-bit integer, middle-endian (swap 16-bit words)"""
        swapped = data[2:4] + data[0:2]
        return struct.unpack('>i', swapped)[0]

    @staticmethod
    def pack_int32_BADC(value):
        """Pack signed 32-bit integer,
        middle-endian (swap bytes within words)"""
        big_endian = struct.pack('>i', value)
        return big_endian[1:2] + big_endian[0:1] + big_endian[
            3:4] + big_endian[2:3]

    @staticmethod
    def unpack_int32_BADC(data):
        """Unpack signed 32-bit integer,
        middle-endian (swap bytes within words)"""
        swapped = data[1:2] + data[0:1] + data[3:4] + data[2:3]
        return struct.unpack('>i', swapped)[0]

    @staticmethod
    def pack_float32_ABCD(value):
        """Pack 32-bit float, big-endian"""
        return struct.pack('>f', value)

    @staticmethod
    def unpack_float32_ABCD(data):
        """Unpack 32-bit float, big-endian"""
        return struct.unpack('>f', data)[0]

    @staticmethod
    def pack_float32_DCBA(value):
        """Pack 32-bit float, little-endian"""
        return struct.pack('<f', value)

    @staticmethod
    def unpack_float32_DCBA(data):
        """Unpack 32-bit float, little-endian"""
        return struct.unpack('<f', data)[0]

    @staticmethod
    def pack_float32_CDAB(value):
        """Pack 32-bit float, middle-endian (swap 16-bit words)"""
        big_endian = struct.pack('>f', value)
        return big_endian[2:4] + big_endian[0:2]

    @staticmethod
    def unpack_float32_CDAB(data):
        """Unpack 32-bit float, middle-endian (swap 16-bit words)"""
        swapped = data[2:4] + data[0:2]
        return struct.unpack('>f', swapped)[0]

    @staticmethod
    def pack_float32_BADC(value):
        """Pack 32-bit float, middle-endian (swap bytes within words)"""
        big_endian = struct.pack('>f', value)
        return big_endian[1:2] + big_endian[0:1] + big_endian[
            3:4] + big_endian[2:3]

    @staticmethod
    def unpack_float32_BADC(data):
        """Unpack 32-bit float, middle-endian (swap bytes within words)"""
        swapped = data[1:2] + data[0:1] + data[3:4] + data[2:3]
        return struct.unpack('>f', swapped)[0]

    @staticmethod
    def pack_bcd16(value):
        """Pack 16-bit BCD (Binary Coded Decimal)"""
        if not 0 <= value <= 9999:
            raise ValueError("BCD16 value must be between 0 and 9999")

        # Convert to 4 BCD digits
        digit1 = (value // 1000) % 10
        digit2 = (value // 100) % 10
        digit3 = (value // 10) % 10
        digit4 = value % 10

        # Pack as two bytes: high byte = digit1|digit2,
        # low byte = digit3|digit4
        high_byte = (digit1 << 4) | digit2
        low_byte = (digit3 << 4) | digit4

        return struct.pack('BB', high_byte, low_byte)

    @staticmethod
    def unpack_bcd16(data):
        """Unpack 16-bit BCD (Binary Coded Decimal)"""
        high_byte, low_byte = struct.unpack('BB', data)

        # Extract BCD digits
        digit1 = (high_byte >> 4) & 0x0F
        digit2 = high_byte & 0x0F
        digit3 = (low_byte >> 4) & 0x0F
        digit4 = low_byte & 0x0F

        # Validate BCD digits
        if any(digit > 9 for digit in [digit1, digit2, digit3, digit4]):
            raise ValueError("Invalid BCD data")

        return digit1 * 1000 + digit2 * 100 + digit3 * 10 + digit4

    @staticmethod
    def pack_bcd32(value):
        """Pack 32-bit BCD (Binary Coded Decimal)"""
        if not 0 <= value <= 99999999:
            raise ValueError("BCD32 value must be between 0 and 99999999")

        # Convert to 8 BCD digits
        digits = []
        temp = value
        for i in range(8):
            digits.append(temp % 10)
            temp //= 10
        digits.reverse()  # Most significant digit first

        # Pack as four bytes
        bytes_data = []
        for i in range(0, 8, 2):
            byte_val = (digits[i] << 4) | digits[i + 1]
            bytes_data.append(byte_val)

        return struct.pack('BBBB', *bytes_data)

    @staticmethod
    def unpack_bcd32(data):
        """Unpack 32-bit BCD (Binary Coded Decimal)"""
        byte1, byte2, byte3, byte4 = struct.unpack('BBBB', data)

        # Extract BCD digits from each byte
        digits = []
        for byte_val in [byte1, byte2, byte3, byte4]:
            high_digit = (byte_val >> 4) & 0x0F
            low_digit = byte_val & 0x0F

            # Validate BCD digits
            if high_digit > 9 or low_digit > 9:
                raise ValueError("Invalid BCD data")

            digits.extend([high_digit, low_digit])

        # Convert BCD digits to integer
        result = 0
        for digit in digits:
            result = result * 10 + digit

        return result


# Example usage and test functions
def test_all_formats():
    """Test all data format conversions"""
    handler = ModbusDataHandler()

    print("Testing Modbus Data Handler")
    print("=" * 40)

    # Test uint16
    test_val_16 = 0x1234
    print(f"\nTesting uint16 with value 0x{test_val_16:04X} ({test_val_16})")

    packed_ab = handler.pack_uint16_AB(test_val_16)
    packed_ba = handler.pack_uint16_BA(test_val_16)
    print(f"uint16_AB packed: {packed_ab.hex().upper()}")
    print(f"uint16_BA packed: {packed_ba.hex().upper()}")
    print(f"uint16_AB unpacked: {handler.unpack_uint16_AB(packed_ab)}")
    print(f"uint16_BA unpacked: {handler.unpack_uint16_BA(packed_ba)}")

    # Test uint32
    test_val_32 = 0x12345678
    print(f"\nTesting uint32 with value 0x{test_val_32:08X} ({test_val_32})")

    packed_abcd = handler.pack_uint32_ABCD(test_val_32)
    packed_dcba = handler.pack_uint32_DCBA(test_val_32)
    packed_cdab = handler.pack_uint32_CDAB(test_val_32)
    packed_badc = handler.pack_uint32_BADC(test_val_32)

    print(f"uint32_ABCD packed: {packed_abcd.hex().upper()}")
    print(f"uint32_DCBA packed: {packed_dcba.hex().upper()}")
    print(f"uint32_CDAB packed: {packed_cdab.hex().upper()}")
    print(f"uint32_BADC packed: {packed_badc.hex().upper()}")

    unpacked = handler.unpack_uint32_ABCD(packed_abcd)
    print(f"uint32_ABCD unpacked: 0x{unpacked:08X}")
    unpacked = handler.unpack_uint32_DCBA(packed_dcba)
    print(f"uint32_DCBA unpacked: 0x{unpacked:08X}")
    unpacked = handler.unpack_uint32_CDAB(packed_cdab)
    print(f"uint32_CDAB unpacked: 0x{unpacked:08X}")
    unpacked = handler.unpack_uint32_BADC(packed_badc)
    print(f"uint32_BADC unpacked: 0x{unpacked:08X}")

    # Test signed integers
    test_int16 = -1234
    test_int32 = -123456789

    print(f"\nTesting int16 with value {test_int16}")
    unpacked = handler.unpack_int16_AB(handler.pack_int16_AB(test_int16))
    print(f"int16_AB: {unpacked}")
    unpacked = handler.unpack_int16_BA(handler.pack_int16_BA(test_int16))
    print(f"int16_BA: {unpacked}")

    print(f"\nTesting int32 with value {test_int32}")
    unpacked = handler.unpack_int32_ABCD(handler.pack_int32_ABCD(test_int32))
    print(f"int32_ABCD: {unpacked}")
    unpacked = handler.unpack_int32_DCBA(handler.pack_int32_DCBA(test_int32))
    print(f"int32_DCBA: {unpacked}")
    unpacked = handler.unpack_int32_CDAB(handler.pack_int32_CDAB(test_int32))
    print(f"int32_CDAB: {unpacked}")
    unpacked = handler.unpack_int32_BADC(handler.pack_int32_BADC(test_int32))
    print(f"int32_BADC: {unpacked}")

    # Test floats
    test_float = 123.456
    print(f"\nTesting float32 with value {test_float}")

    packed_f_abcd = handler.pack_float32_ABCD(test_float)
    packed_f_dcba = handler.pack_float32_DCBA(test_float)
    packed_f_cdab = handler.pack_float32_CDAB(test_float)
    packed_f_badc = handler.pack_float32_BADC(test_float)

    print(f"float32_ABCD packed: {packed_f_abcd.hex().upper()}")
    print(f"float32_DCBA packed: {packed_f_dcba.hex().upper()}")
    print(f"float32_CDAB packed: {packed_f_cdab.hex().upper()}")
    print(f"float32_BADC packed: {packed_f_badc.hex().upper()}")

    packed = handler.unpack_float32_ABCD(packed_f_abcd)
    print(f"float32_ABCD unpacked: {packed:.6f}")
    packed = handler.unpack_float32_DCBA(packed_f_dcba)
    print(f"float32_DCBA unpacked: {packed:.6f}")
    packed = handler.unpack_float32_CDAB(packed_f_cdab)
    print(f"float32_CDAB unpacked: {packed:.6f}")
    packed = handler.unpack_float32_BADC(packed_f_badc)
    print(f"float32_BADC unpacked: {packed:.6f}")

    # Test BCD
    test_bcd16 = 1234
    test_bcd32 = 12345678

    print(f"\nTesting BCD16 with value {test_bcd16}")
    packed_bcd16 = handler.pack_bcd16(test_bcd16)
    print(f"BCD16 packed: {packed_bcd16.hex().upper()}")
    print(f"BCD16 unpacked: {handler.unpack_bcd16(packed_bcd16)}")

    print(f"\nTesting BCD32 with value {test_bcd32}")
    packed_bcd32 = handler.pack_bcd32(test_bcd32)
    print(f"BCD32 packed: {packed_bcd32.hex().upper()}")
    print(f"BCD32 unpacked: {handler.unpack_bcd32(packed_bcd32)}")


if __name__ == "__main__":
    test_all_formats()
