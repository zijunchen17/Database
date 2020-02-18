from lstore.page import *
from lstore.config import *
import math

class Page_Range:

    def __init__(self, all_columns):
        self.full = False
        self.base_pages = [[Page()] for _ in range(all_columns)]
        self.tail_pages = [[Page()] for _ in range(all_columns)]
        self.__occupy_0_rid_slot()
    
    def has_capacity(self):
        if len(self.base_pages[0]) == BASE_PAGES_PER_RANGE and not self.base_pages[0][-1].has_capacity():
            self.full = True

        return not self.full
    
    def print_page_range(self):
        for j, page in enumerate(self.base_pages[0]):
            for i in range(0, PAGE_SIZE // RECORD_SIZE):
                print(self.base_pages[0][j].read(i), self.base_pages[1][j].read(i), self.base_pages[3][j].read(i), self.base_pages[4][j].read(i), self.base_pages[5][j].read(i), self.base_pages[6][j].read(i), self.base_pages[7][j].read(i), self.base_pages[8][j].read(i))
            print('==================page break====================')
    
        for j, page in enumerate(self.tail_pages[0]):
            for i in range(0, PAGE_SIZE // RECORD_SIZE):
                print(self.tail_pages[0][j].read(i), self.tail_pages[1][j].read(i), self.tail_pages[2][j].read(i), self.tail_pages[3][j].read(i), self.tail_pages[4][j].read(i), self.tail_pages[5][j].read(i), self.tail_pages[6][j].read(i), self.tail_pages[7][j].read(i), self.tail_pages[8][j].read(i))
            print('==================page break====================')
    def get_base_page(self, index):
        return self.base_pages[index]
    
    def get_base_page_index(self, rid):
        return rid // (PAGE_SIZE // RECORD_SIZE)
    
    def get_last_base_page(self):
        if len(self.base_pages[0]) < BASE_PAGES_PER_RANGE and not self.base_pages[0][-1].has_capacity():
            self.__add_base_page()
            print('new base page added')

        return [column[-1] for column in self.base_pages]
    
    def get_last_base_page_index(self):
        return len(self.base_pages) - 1

    def get_tail_page(self, index):
        return self.tail_pages[index]
    
    def get_last_tail_page(self):
        if not self.tail_pages[0][-1].has_capacity():
            self.__add_tail_page()
        
        return [column[-1] for column in self.tail_pages]
    
    def __occupy_0_rid_slot(self):
        for column in self.base_pages:
            column[0].write(SPECIAL_NULL_VALUE, 0)

    def __add_base_page(self):
        for page_list in self.base_pages:
            page_list.append(Page())

    def __add_tail_page(self):
        for page_list in self.tail_pages:
            page_list.append(Page())
    


        