# Global Setting for the Database
# PageSize, StartRID, etc..

RECORD_SIZE = 8
PAGE_SIZE = 4096
BASE_PAGES_PER_RANGE = 16
PAGE_RANGE_SIZE = (PAGE_SIZE // RECORD_SIZE - 1) * BASE_PAGES_PER_RANGE

BYTE_ORDER = 'little'
BITE_SHIFT = 9
SPECIAL_NULL_VALUE = 2**64 -10
# SPECIAL_NULL_VALUE = b'\x00'
