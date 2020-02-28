from lstore.page import *
from lstore.config import *
import math

class Page_Range:

    def __init__(self, table_name, page_range_index, all_columns):
        self.table_name = table_name

        self.page_range_index = page_range_index

        self.all_columns = all_columns

        self.pinned = False

        self.dirty = False

        self.base_pages = [ [] for _ in range(self.all_columns)]

        self.tail_pages = [ [] for _ in range(self.all_columns)]
    
    # Method for printing out the contents of a page range, useful for debugging
    def print_page_range(self):
        print("base page index:", self.base_pages[0][-1].page_index)
        print('base page column index', self.base_pages[1][-1].column_index)
        print('base page type', self.base_pages[0][-1].page_type)
        print("num of base pages:", len(self.base_pages[0]))
        print("page_range_index:", self.base_pages[0][-1].page_range_index)

        print("tail page index:", self.tail_pages[0][-1].page_index)
        print('tail page column index', self.tail_pages[1][-1].column_index)
        print('tail page type', self.tail_pages[0][-1].page_type)
        print("num of tail pages:", len(self.tail_pages[0]))
        print("page_range_index:", self.tail_pages[0][-1].page_range_index)


        for j, page in enumerate(self.base_pages[0]):
            for i in range(0, PAGE_SIZE // RECORD_SIZE):
                for k in range(self.all_columns):
                    print(self.base_pages[k][j].read(i),end=' ')
                print('\n',end='')
            print(f'=============base page{j} break====================')

        for j, page in enumerate(self.tail_pages[0]):
            for i in range(0, PAGE_SIZE // RECORD_SIZE):
                for k in range(self.all_columns):
                    print(self.tail_pages[k][j].read(i),end=' ')
                print('\n',end='')
            print(f'=============tail page{j} break====================')

            
    def get_base_page(self, index):
        return [column[index] for column in self.base_pages]
    
    def get_base_page_index(self, base_rid):
        return (base_rid - 1) // (PAGE_SIZE // RECORD_SIZE)

    def get_base_physical_offset(self, base_rid):
        return (base_rid - 1) % (PAGE_SIZE // RECORD_SIZE)
    
    def get_last_base_page_index(self):
        return len(self.base_pages) - 1

    def get_tail_page(self, index):
        return [column[index] for column in self.tail_pages]
    
    def get_tail_page_index(self, tail_rid):
        return (2**64 - tail_rid - PAGE_SIZE // RECORD_SIZE) // (PAGE_SIZE // RECORD_SIZE)**2 % (NUM_TAILS_BEFORE_MERGE * 2)

    def get_first_tail_page_with_available_space(self):
        for page_index, page in enumerate(self.tail_pages[0]):
            if page.has_capacity():
                return [column[page_index] for column in self.tail_pages]

        # If no tail page that has space is found
        return None
    '''
    def __add_base_page(self):
        for i, page_list in enumerate(self.base_pages):
            page_list.append(Page(len(self.base_pages[0]), self.page_range_index, BASE_PAGE_TYPE, i))

    def __add_tail_page(self):
        for i, page_list in enumerate(self.tail_pages):
            page_list.append(Page(len(self.tail_pages[0]), self.page_range_index, TAIL_PAGE_TYPE, i))
    '''