from lstore.config import *

def get_page_range_index(rid):
    return (rid - 1) // (PAGE_SIZE // RECORD_SIZE * BASE_PAGES_PER_RANGE)


def parse_page_attributes_from_filename(string):
    # Example input: "ECS165/Grades/page_range0/base/column1"

    def find_last(target,input):
        for index in reversed(range(len(input))):
            if input[index] == target:
                return index

    # table_name = range_num = page_type = col_num = ''
    tokenized = string.split('/')
    table_name = tokenized[1]

    before_e =  find_last('e',tokenized[2])
    range_num = int(tokenized[2][before_e+1:])

    page_type = tokenized[3]
    col_num = int(tokenized[4][6:])

    return (table_name, range_num, page_type, col_num)
