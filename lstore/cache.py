from lstore.db import Database
from lstore.table import *
from lstore.page import *
from lstore.config import *
from lstore.utils import
from collections import OrderedDict


class Cache:

        def __init__(self):
                self.cache_size = CACHE_SIZE
                self.cache = OrderedDict()

        def __contains(self, key):
                return key in self.cache
        
        def __num_page_ranges_in_cache(self):
                return len(self.cache)

        def __evict(self):
                evict_key = None
                # Looks for oldest unpinned page range
                for key in iter(self.cache):
                        if self.cache[key].pinned == False:
                                evict_key = key
                                break

                # If we found an unpinned page range to evict
                if evict_key != None:
                        evicted_page_range = self.cache[evict_key]
                        del self.cache[evict_key]

                        if evicted_page_range.dirty == True:
                                '''
                                Write to disk
                                '''
        def __read_disk(self):
        
        def __write_disk(self, page_range):

        def __insert(self, key, page_range):
                # If cache is full, evict oldest unpinned page
                if self.__num_page_ranges_in_cache == CACHE_SIZE:
                        self.__evict()
                
                # If cache is no longer full, start insertion process
                if self.__num_page_ranges_in_cache < CACHE_SIZE:
                        self.cache[key] = page_range

                else: # All page ranges in cache are pinned, can't insert a new page into cache
                        print('Error inserting new page into the bufferpool/cache')
                        print('Cache is full and all page ranges are pinned')

        def add_page_range(self, page_range_index, page_range):
                key = page_range_index
                self.__insert(key, page_range)
        
        def get_page_range(self, key):
                # Check if page range being accessed is already in cache
                # Grab the page range, take it out of cache and reinsert
                # to make it the most recent item in cache 
                if key in self.cache:
                        page_range = self.cache[key]
                        del self.cache[key]
                        self.cache[key] = page_range
                        self.cache[key].pinned = True
                # Grab the page range from disk
                else:
                        '''
                        Read page range from disk
                        '''
                        self.__insert(key, page_range)
                
                return self.cache[key]
        
        # !!! Only close cache when no queries running !!!
        def close_cache(self):
                while self.cache: # As long as cache isn't empty, pop and write each page to disk
                        page_range = self.cache.popitem(False) # False means pop oldest item
                        '''
                         Write page range to disk
                        '''
