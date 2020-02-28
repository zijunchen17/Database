from lstore.config import *

def get_page_range_index(rid):
    return (rid - 1) // (PAGE_SIZE // RECORD_SIZE * BASE_PAGES_PER_RANGE)


def parse_page_attributes_from_filename(path):
    # Example input: "ECS165/Grades/page_range0/base/column1"

    tokenized = path.split('/')
    
    table_name = tokenized[1]
    range_num = int(tokenized[2][10:])
    page_type = tokenized[3]
    col_num = int(tokenized[4][6:])

    return (table_name, range_num, page_type, col_num)