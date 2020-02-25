from lstore.db import Database
from lstore.table import *
from lstore.page import *
from lstore.config import *
from collections import OrderedDict


class Cache:

        def __init__(self, tables):
                self.cache_size = CACHE_SIZE
                self.cache = OrderedDict()
                self.tables = tables

        def __contains(self, key):
                return key in self.cache
        
        def __num_pages_in_cache(self):
                return len(self.cache)

        def __evict(self):
                evict_key = None
                # Looks for oldest unpinned page
                for key in iter(self.cache):
                        if self.cache[key].pinned == False:
                                evict_key = key
                                break

                # If we found an unpinned page to evict
                if evict_key != None:
                        evicted_page = self.cache[evict_key]
                        del self.cache[evict_key]

                        if evicted_page.dirty == True:
                                '''
                                Write to disk if dirty
                                '''
        def __read_disk(self):
        
        def __write_disk(self, page):

        def __insert(self, key, page):
                key = None
                # If cache is full, evict oldest unpinned page
                if self.__num_pages_in_cache == CACHE_SIZE:
                        self.__evict()
                
                # If cache is no longer full, start insertion process
                if self.__num_pages_in_cache == CACHE_SIZE:
                        '''
                        Build key for the page
                        '''
                        key = new_key
                        self.cache[key] = page

                else: # All pages in cache are pinned, can't insert a new page into cache
                        print('Error inserting new page into the bufferpool/cache')
                        print('Cache is full and all pages are pinned')
                
                return key

        
        def get_page(self, key):
                # Check if page being accessed is already in cache
                if key in self.cache:
                        self.cache[key].pinned = True
                # Grab the page from disk
                else:
                        '''
                        Read page from disk
                        '''
                        key = self.__insert(page)


                return self.cache[key]


        def close_cache(self):
                while self.cache: # As long as cache isn't empty, pop and write each page to disk
                        page = self.cache.popitem(False) # False means pop oldest item
                        '''
                         Write page to disk
                        '''
