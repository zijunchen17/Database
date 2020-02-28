from lstore.config import *

def get_page_range_index(rid):
    return (rid - 1) // (PAGE_SIZE // RECORD_SIZE * BASE_PAGES_PER_RANGE)