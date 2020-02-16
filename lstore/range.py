from lstore.page import *
from lstore.config import *
import math

class Page_Range:

    def __init__(self, all_columns):

        self.full = 0
        self.base_pages = [[Page()] for _ in range(all_columns)]
        self.tail_pages = [[Page()] for _ in range(all_columns)]

    def getBasePageIndex(self, rid):

        return rid // (PAGE_SIZE // RECORD_SIZE)
    
    def getBasePage(self, index):

        return self.base_pages[index]
    
    def getLastBasePage(self):

        return [column[-1] for column in self.base_pages]

    def __addBasePage(self):

        if not self.full:
            if not self.base_pages[0][-1].has_capacity():
                for page_list in self.base_pages:
                    page_list.append(Page())

    def __addTailPage(self):

        if not self.tail_pages[0][-1].has_capacity():
            for page_list in self.tail_pages:
                page_list.append(Page())
        


        