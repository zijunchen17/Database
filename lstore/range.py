from lstore.page import *
from lstore.config import *
import math

class Page_Range:

    def __init__(self, page_range_index, all_columns):
        self.page_range_index = page_range_index
        self.full = False
        self.base_pages = [[Page(self.page_range_index, BASE_PAGE_TYPE, i)] for i in range(all_columns)]
        self.tail_pages = [[Page(self.page_range_index, TAIL_PAGE_TYPE, i)] for i in range(all_columns)]
        self.__occupy_tps_slot()

    def has_capacity(self):
        if len(self.base_pages[0]) == BASE_PAGES_PER_RANGE and not self.base_pages[0][-1].has_capacity():
            self.full = True

        return self.full
    
    # Method for printing out the contents of a page range, useful for debugging
    def print_page_range(self):
        for j, page in enumerate(self.base_pages[0]):
            for i in range(0, PAGE_SIZE // RECORD_SIZE):
                print(self.base_pages[0][j].read(i), self.base_pages[1][j].read(i), self.base_pages[2][j].read(i), self.base_pages[3][j].read(i), self.base_pages[4][j].read(i), self.base_pages[5][j].read(i), self.base_pages[6][j].read(i), self.base_pages[7][j].read(i), self.base_pages[8][j].read(i))
            print('==================page break====================')
    
        for j, page in enumerate(self.tail_pages[0]):
            for i in range(0, PAGE_SIZE // RECORD_SIZE - 1):
                print(self.tail_pages[0][j].read(i), self.tail_pages[1][j].read(i), self.tail_pages[2][j].read(i), self.tail_pages[3][j].read(i), self.tail_pages[4][j].read(i), self.tail_pages[5][j].read(i), self.tail_pages[6][j].read(i), self.tail_pages[7][j].read(i), self.tail_pages[8][j].read(i))
            print('==================page break====================')
            
    def get_base_page(self, index):
        return self.base_pages[index]
    
    def get_base_page_index(self, rid):
        return (rid - 1) // (PAGE_SIZE // RECORD_SIZE - 1) % 16
    
    def get_last_base_page(self):

        # Check if physical page is filled up to the tps row, if it is, increment num_records.
        # The page returns false when calling page.has_capacity()
        if (self.base_pages[0][-1].num_records == PAGE_SIZE // RECORD_SIZE - 1):
            self.base_pages[0][-1].num_records += 1
        
        # If page range hasn't exceeded base page capacity and the last base page is full,
        # allocate a new base page
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
    
    # Fill last row of physical page with null value to reserve it for tps
    def __occupy_tps_slot(self):
        for column in self.base_pages:
            column[-1].write(SPECIAL_NULL_VALUE, PAGE_SIZE // RECORD_SIZE - 1)

    def __add_base_page(self):
        for i, page_list in enumerate(self.base_pages):
            page_list.append(Page(self.page_range_index, BASE_PAGE_TYPE, i))
            self.__occupy_tps_slot()

    def __add_tail_page(self):
        for i, page_list in enumerate(self.tail_pages):
            page_list.append(Page(self.page_range_index, TAIL_PAGE_TYPE, i))
    


        