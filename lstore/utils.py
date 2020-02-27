from lstore.config import *

def get_page_range_index(rid):
    return rid // (PAGE_SIZE // RECORD_SIZE * BASE_PAGES_PER_RANGE)