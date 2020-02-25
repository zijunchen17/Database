from lstore.page import *
from lstore.config import *
import math

class Page_Range:

    def __init__(self, page_range_index, all_columns):
        self.page_range_index = page_range_index

        self.base_pages = [[Page(0, self.page_range_index, BASE_PAGE_TYPE, i)] for i in range(all_columns)]
        self.tail_pages = [[Page(0, self.page_range_index, TAIL_PAGE_TYPE, i)] for i in range(all_columns)]

    def has_capacity(self):
        if len(self.base_pages[0]) == BASE_PAGES_PER_RANGE and not self.base_pages[0][-1].has_capacity():
            return False

        return True
    
    # Method for printing out the contents of a page range, useful for debugging
    def print_page_range(self):
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


        # for j, page in enumerate(self.base_pages[0]):
        #     for i in range(0, PAGE_SIZE // RECORD_SIZE):
        #         print(self.base_pages[0][j].read(i), self.base_pages[1][j].read(i), self.base_pages[2][j].read(i), self.base_pages[3][j].read(i), self.base_pages[4][j].read(i), self.base_pages[5][j].read(i), self.base_pages[6][j].read(i), self.base_pages[7][j].read(i), self.base_pages[8][j].read(i), self.base_pages[9][j].read(i))
        #     print('==================page break====================')

        # for j, page in enumerate(self.tail_pages[0]):
        #     for i in range(0, PAGE_SIZE // RECORD_SIZE - 1):
        #         print(self.tail_pages[0][j].read(i), self.tail_pages[1][j].read(i), self.tail_pages[2][j].read(i), self.tail_pages[3][j].read(i), self.tail_pages[4][j].read(i), self.tail_pages[5][j].read(i), self.tail_pages[6][j].read(i), self.tail_pages[7][j].read(i), self.tail_pages[8][j].read(i), self.tail_pages[9][j].read(i))
        #     print('==================page break====================')
            
    def get_base_page(self, index):
        return self.base_pages[index]
    
    def get_base_page_index(self, rid):
        return rid // (PAGE_SIZE // RECORD_SIZE)
    
    def get_last_base_page(self):

        # If page range hasn't exceeded base page capacity and the last base page is full,
        # allocate a new base page
        if len(self.base_pages[0]) < BASE_PAGES_PER_RANGE and not self.base_pages[0][-1].has_capacity():
            self.__add_base_page()
            #print('new base page added')

        return [column[-1] for column in self.base_pages]
    
    def get_last_base_page_index(self):
        return len(self.base_pages) - 1

    def get_tail_page(self, index):
        return self.tail_pages[index]
    
    def get_last_tail_page(self):
        if not self.tail_pages[0][-1].has_capacity():
            self.__add_tail_page()
        
        return [column[-1] for column in self.tail_pages]

    def __add_base_page(self):
        for i, page_list in enumerate(self.base_pages):
            page_list.append(Page(len(self.base_pages[0]), self.page_range_index, BASE_PAGE_TYPE, i))

    def __add_tail_page(self):
        for i, page_list in enumerate(self.tail_pages):
            page_list.append(Page(len(self.tail_pages[0]), self.page_range_index, TAIL_PAGE_TYPE, i))
  


        