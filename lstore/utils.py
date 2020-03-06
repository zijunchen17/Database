from lstore.config import *

def get_page_range_index(rid):
    return (rid - 1) // (PAGE_SIZE // RECORD_SIZE * BASE_PAGES_PER_RANGE)

def get_base_page_index(base_rid):
    return (base_rid - 1) // (PAGE_SIZE // RECORD_SIZE) % BASE_PAGES_PER_RANGE

def get_base_physical_offset(base_rid):
    return (base_rid - 1) % (PAGE_SIZE // RECORD_SIZE)

def parse_page_attributes_from_filename(path):
    # Example input: "ECS165/Grades/page_range0/base/column1"

    tokenized = path.split('/')
    
    table_name = tokenized[1]
    range_num = int(tokenized[2][10:])
    page_type = tokenized[3]
    col_num = int(tokenized[4][6:])

    return (table_name, range_num, page_type, col_num)

# Method for printing out the contents of a page range, useful for debugging
def print_page_range(table, page_range_index):

    # print("base page index:", self.base_pages[0][-1].page_index)
    # print('base page column index', self.base_pages[1][-1].column_index)
    # print('base page type', self.base_pages[0][-1].page_type)
    # print("num of base pages:", len(self.base_pages[0]))
    # print("page_range_index:", self.base_pages[0][-1].page_range_index)

    # print("tail page index:", self.tail_pages[0][-1].page_index)
    # print('tail page column index', self.tail_pages[1][-1].column_index)
    # print('tail page type', self.tail_pages[0][-1].page_type)
    # print("num of tail pages:", len(self.tail_pages[0]))
    # print("page_range_index:", self.tail_pages[0][-1].page_range_index)
    
    for i in range(0, BASE_PAGES_PER_RANGE):
        for j in range(0, PAGE_SIZE // RECORD_SIZE):
            for k in range(table.all_columns):
                physical_page = table.bufferpool.get_physical_page(table, page_range_index, 'base', i, k)
                print(physical_page.read(j),end=' ')
                physical_page.pinned = False
            print('\n',end='')
        print(f'=============base page{i} break====================')

    for i in range(0, table.tail_page_index_directory[page_range_index] + 1):
        for j in range(0, PAGE_SIZE // RECORD_SIZE):
            for k in range(table.all_columns):
                physical_page = table.bufferpool.get_physical_page(table, page_range_index, 'tail', i, k)
                print(physical_page.read(j),end=' ')
                physical_page.pinned = False
            print('\n',end='')
        print(f'=============tail page{i} break====================')
    print(f'=============page range{page_range_index} break====================')