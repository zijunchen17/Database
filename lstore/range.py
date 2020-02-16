from lstore.page import *
from lstore.config import *
import math

class Page_Range:

    def __init__(self):

        self.base_pages = [ [[Page()] for _ in range(self.all_columns)] ] * BASE_PAGES_PER_RANGE
        self.tail_pages = [ [[Page()] for _ in range(self.all_columns)] ]
        
        def getBasePageIndex(self, rid):

            return rid // (PAGE_SIZE // RECORD_SIZE)
        
        def getBasePage(self, index):

            return self.base_pages[index]
        
        


        