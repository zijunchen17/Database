from lstore.page import *
from lstore.config import *
import math

class Page_Range:

    def __init__(self, all_columns):
        self.full = False
        self.base_pages = [[Page()] for _ in range(all_columns)]
        self.tail_pages = [[Page()] for _ in range(all_columns)]
    
    def has_capacity(self):
        if len(self.base_pages[0]) == BASE_PAGES_PER_RANGE and not self.base_pages[0][-1].has_capacity():
            self.full = True

        return not self.full

    def get_base_page(self, index):
        return self.base_pages[index]
    
    def get_base_page_index(self, rid):
        return rid // (PAGE_SIZE // RECORD_SIZE)
    
    def get_last_base_page(self):
        if not self.base_pages[0][-1].has_capacity():
            self.__add_base_page():

        return [column[-1] for column in self.base_pages]
    
    def get_last_base_page_index(self):
        return len(self.base_pages) - 1

    def __add_base_page(self):
        for page_list in self.base_pages:
            page_list.append(Page())

    def __add_tail_page(self):

        if not self.tail_pages[0][-1].has_capacity():
            for page_list in self.tail_pages:
                page_list.append(Page())
        


        