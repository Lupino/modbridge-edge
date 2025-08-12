import struct

def modbus_crc16_table(data):
    """
    Calculate Modbus CRC-16 using lookup table method (fastest)

    Args:
        data: bytes or bytearray to calculate CRC for

    Returns:
        int: CRC-16 value (0-65535)
    """
    # CRC-16 lookup table for Modbus
    crc_table = [
        0x0000, 0xC0C1, 0xC181, 0x0140, 0xC301, 0x03C0, 0x0280, 0xC241,
        0xC601, 0x06C0, 0x0780, 0xC741, 0x0500, 0xC5C1, 0xC481, 0x0440,
        0xCC01, 0x0CC0, 0x0D80, 0xCD41, 0x0F00, 0xCFC1, 0xCE81, 0x0E40,
        0x0A00, 0xCAC1, 0xCB81, 0x0B40, 0xC901, 0x09C0, 0x0880, 0xC841,
        0xD801, 0x18C0, 0x1980, 0xD941, 0x1B00, 0xDBC1, 0xDA81, 0x1A40,
        0x1E00, 0xDEC1, 0xDF81, 0x1F40, 0xDD01, 0x1DC0, 0x1C80, 0xDC41,
        0x1400, 0xD4C1, 0xD581, 0x1540, 0xD701, 0x17C0, 0x1680, 0xD641,
        0xD201, 0x12C0, 0x1380, 0xD341, 0x1100, 0xD1C1, 0xD081, 0x1040,
        0xF001, 0x30C0, 0x3180, 0xF141, 0x3300, 0xF3C1, 0xF281, 0x3240,
        0x3600, 0xF6C1, 0xF781, 0x3740, 0xF501, 0x35C0, 0x3480, 0xF441,
        0x3C00, 0xFCC1, 0xFD81, 0x3D40, 0xFF01, 0x3FC0, 0x3E80, 0xFE41,
        0xFA01, 0x3AC0, 0x3B80, 0xFB41, 0x3900, 0xF9C1, 0xF881, 0x3840,
        0x2800, 0xE8C1, 0xE981, 0x2940, 0xEB01, 0x2BC0, 0x2A80, 0xEA41,
        0xEE01, 0x2EC0, 0x2F80, 0xEF41, 0x2D00, 0xEDC1, 0xEC81, 0x2C40,
        0xE401, 0x24C0, 0x2580, 0xE541, 0x2700, 0xE7C1, 0xE681, 0x2640,
        0x2200, 0xE2C1, 0xE381, 0x2340, 0xE101, 0x21C0, 0x2080, 0xE041,
        0xA001, 0x60C0, 0x6180, 0xA141, 0x6300, 0xA3C1, 0xA281, 0x6240,
        0x6600, 0xA6C1, 0xA781, 0x6740, 0xA501, 0x65C0, 0x6480, 0xA441,
        0x6C00, 0xACC1, 0xAD81, 0x6D40, 0xAF01, 0x6FC0, 0x6E80, 0xAE41,
        0xAA01, 0x6AC0, 0x6B80, 0xAB41, 0x6900, 0xA9C1, 0xA881, 0x6840,
        0x7800, 0xB8C1, 0xB981, 0x7940, 0xBB01, 0x7BC0, 0x7A80, 0xBA41,
        0xBE01, 0x7EC0, 0x7F80, 0xBF41, 0x7D00, 0xBDC1, 0xBC81, 0x7C40,
        0xB401, 0x74C0, 0x7580, 0xB541, 0x7700, 0xB7C1, 0xB681, 0x7640,
        0x7200, 0xB2C1, 0xB381, 0x7340, 0xB101, 0x71C0, 0x7080, 0xB041,
        0x5000, 0x90C1, 0x9181, 0x5140, 0x9301, 0x53C0, 0x5280, 0x9241,
        0x9601, 0x56C0, 0x5780, 0x9741, 0x5500, 0x95C1, 0x9481, 0x5440,
        0x9C01, 0x5CC0, 0x5D80, 0x9D41, 0x5F00, 0x9FC1, 0x9E81, 0x5E40,
        0x5A00, 0x9AC1, 0x9B81, 0x5B40, 0x9901, 0x59C0, 0x5880, 0x9841,
        0x8801, 0x48C0, 0x4980, 0x8941, 0x4B00, 0x8BC1, 0x8A81, 0x4A40,
        0x4E00, 0x8EC1, 0x8F81, 0x4F40, 0x8D01, 0x4DC0, 0x4C80, 0x8C41,
        0x4400, 0x84C1, 0x8581, 0x4540, 0x8701, 0x47C0, 0x4680, 0x8641,
        0x8201, 0x42C0, 0x4380, 0x8341, 0x4100, 0x81C1, 0x8081, 0x4040
    ]

    crc = 0xFFFF
    for byte in data:
        tbl_idx = (crc ^ byte) & 0xFF
        crc = ((crc >> 8) ^ crc_table[tbl_idx]) & 0xFFFF

    return crc


def modbus_crc16_simple(data):
    """
    Calculate Modbus CRC-16 using simple polynomial method

    Args:
        data: bytes or bytearray to calculate CRC for

    Returns:
        int: CRC-16 value (0-65535)
    """
    crc = 0xFFFF

    for byte in data:
        crc ^= byte
        for _ in range(8):
            if crc & 0x0001:
                crc >>= 1
                crc ^= 0xA001  # Modbus polynomial (reversed)
            else:
                crc >>= 1

    return crc


def add_modbus_crc(data):
    """
    Add Modbus CRC to data packet (appends CRC bytes in little-endian format)

    Args:
        data: bytes or bytearray of the message

    Returns:
        bytes: Original data with CRC appended
    """
    if isinstance(data, str):
        data = data.encode()

    crc = modbus_crc16_table(data)
    # Modbus CRC is transmitted low byte first (little-endian)
    crc_bytes = struct.pack('<H', crc)  # '<H' = little-endian unsigned short

    return data + crc_bytes


def verify_modbus_crc(data_with_crc):
    """
    Verify Modbus CRC of a complete packet

    Args:
        data_with_crc: bytes containing message + CRC

    Returns:
        bool: True if CRC is valid, False otherwise
    """
    if len(data_with_crc) < 3:  # Need at least 1 data byte + 2 CRC bytes
        return False

    # Split data and CRC
    data = data_with_crc[:-2]
    received_crc = struct.unpack('<H', data_with_crc[-2:])[0]

    # Calculate CRC of data
    calculated_crc = modbus_crc16_table(data)

    return received_crc == calculated_crc


def modbus_crc_hex_string(hex_string):
    """
    Calculate CRC for hex string input

    Args:
        hex_string: String of hex values (e.g., "010300000001")

    Returns:
        tuple: (crc_int, crc_hex_string, complete_packet_hex)
    """
    # Remove spaces and convert to bytes
    clean_hex = hex_string.replace(' ', '').replace(':', '')
    data = bytes.fromhex(clean_hex)

    crc = modbus_crc16_table(data)
    crc_hex = f"{crc:04X}"

    # Create complete packet with CRC (little-endian)
    complete_packet = add_modbus_crc(data)
    complete_hex = complete_packet.hex().upper()

    return crc, crc_hex, complete_hex


# Example usage and test cases
if __name__ == "__main__":
    # Test case 1: Simple Modbus RTU read holding registers command
    # Slave ID: 01, Function: 03, Start: 0000, Quantity: 0001
    test_data1 = bytes.fromhex("010300000001")
    crc1 = modbus_crc16_table(test_data1)
    packet1 = add_modbus_crc(test_data1)

    print("Test 1 - Read Holding Registers:")
    print(f"Data: {test_data1.hex().upper()}")
    print(f"CRC: {crc1:04X} ({crc1})")
    print(f"Complete packet: {packet1.hex().upper()}")
    print(f"CRC valid: {verify_modbus_crc(packet1)}")
    print()

    # Test case 2: Write single coil command
    # Slave ID: 01, Function: 05, Address: 0000, Value: FF00
    test_data2 = bytes.fromhex("0105000000FF00")
    crc2 = modbus_crc16_table(test_data2)
    packet2 = add_modbus_crc(test_data2)

    print("Test 2 - Write Single Coil:")
    print(f"Data: {test_data2.hex().upper()}")
    print(f"CRC: {crc2:04X} ({crc2})")
    print(f"Complete packet: {packet2.hex().upper()}")
    print(f"CRC valid: {verify_modbus_crc(packet2)}")
    print()

    # Test case 3: Using hex string function
    print("Test 3 - Using hex string function:")
    crc_val, crc_hex, complete = modbus_crc_hex_string("01 03 00 00 00 01")
    print(f"CRC: {crc_hex} ({crc_val})")
    print(f"Complete packet: {complete}")
    print()

    # Performance comparison
    import time

    large_data = b"A" * 1000

    # Time table method
    start = time.time()
    for _ in range(10000):
        modbus_crc16_table(large_data)
    table_time = time.time() - start

    # Time simple method
    start = time.time()
    for _ in range(10000):
        modbus_crc16_simple(large_data)
    simple_time = time.time() - start

    print("Performance comparison (10,000 iterations on 1KB data):")
    print(f"Table method: {table_time:.4f} seconds")
    print(f"Simple method: {simple_time:.4f} seconds")
    print(f"Table method is {simple_time/table_time:.1f}x faster")
