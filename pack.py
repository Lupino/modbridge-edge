import struct

def get_pack_func(format):
    if format == 'uint16_AB':
        return '>H'
    if format == 'uint16_BA':
        return '<H'
    if format == 'uint32_ABCD':
        return '>I'
    if format == 'uint32_DCBA':
        return '<I'
    if format == 'int16_AB':
        return '>h'
    if format == 'int16_BA':
        return '<h'
    if format == 'int32_ABCD':
        return '>i'
    if format == 'int32_DCBA':
        return '<i'
    if format == 'float32_ABCD':
        return '>f'
    if format == 'float32_DCBA':
        return '<f'

    return ''

def pack_uint32_BADC(value32):
    return struct.pack('<H', value32 & 0xFFFF) + struct.pack('<H', (value32 >> 16) & 0xFFFF)

def pack_uint32_CDAB(value32):
    return struct.pack('>H', value32 & 0xFFFF) + struct.pack('>H', (value32 >> 16) & 0xFFFF)

def pack_int32_BADC(value):
    """
    Pack int32 in BADC format (swap bytes within each 16-bit word)
    ABCD -> BADC
    """
    # First pack as big-endian to get ABCD
    abcd_bytes = struct.pack('>i', value)

    # Rearrange bytes: ABCD -> BADC
    a, b, c, d = abcd_bytes
    return bytes([b, a, d, c])


def pack_int32_CDAB(value):
    """
    Pack int32 in CDAB format (swap 16-bit words)
    ABCD -> CDAB
    """
    # First pack as big-endian to get ABCD
    abcd_bytes = struct.pack('>i', value)

    # Rearrange bytes: ABCD -> CDAB
    a, b, c, d = abcd_bytes
    return bytes([c, d, a, b])


def pack_float32_CDAB(value):
    """
    Pack float32 in CDAB format (Middle-endian, swap 16-bit words)
    Swap high and low 16-bit words
    """
    # Pack as big-endian first to get ABCD
    abcd_bytes = struct.pack('>f', value)

    # Rearrange bytes: ABCD -> CDAB
    a, b, c, d = abcd_bytes
    return bytes([c, d, a, b])


def pack_float32_BADC(value):
    """
    Pack float32 in BADC format (Middle-endian, swap bytes within words)
    Swap bytes within each 16-bit word
    """
    # Pack as big-endian first to get ABCD
    abcd_bytes = struct.pack('>f', value)

    # Rearrange bytes: ABCD -> BADC
    a, b, c, d = abcd_bytes
    return bytes([b, a, d, c])


def pack_value(value, format):
    func = get_pack_func(format)

    if func:
        return struct.pack(func, value)

    if format == 'uint32_CDAB':
        return pack_uint32_CDAB(value)

    if format == 'uint32_BADC':
        return pack_uint32_BADC(value)

    if format == 'int32_CDAB':
        return pack_int32_CDAB(value)

    if format == 'int32_BADC':
        return pack_int32_BADC(value)

    if format == 'float32_CDAB':
        return pack_float32_CDAB(value)

    if format == 'float32_BADC':
        return pack_float32_BADC(value)

    return ''
