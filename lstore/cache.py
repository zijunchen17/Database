from lstore.db import Database
from lstore.table import *
from lstore.page import *
from lstore.config import *
import datetime


class Cache:

        def __init__(self, tables):
                self.cache_size = NUM_PAGES
                self.cache = {}
                self.tables = tables

        def __contains(self, key):
                return key in self.cache

        def __evict(self):
                lru_page = None
                for key in self.cache:
                        if lru_page == None:
                                lru_page = key
                        # Compares time accessed between the key and the current oldest item
                        # and older date and time is less than a newer date and time
                        elif self.cache[key][1] < self.cache[lru_page][1]:
                                lru_page = key
                
                self.cache.pop(lru_page)
        
        def __insert(self, page):
                if len(self.cache) == NUM_PAGES:
                        self.__evict()
                
                key = str(page.table_name) + '/page_range' + str(page.range_index)

                self.cache[key] = (page, datetime.datetime.now())
                
                
                
                