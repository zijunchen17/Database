from lstore.table import Table
from lstore.range import Page_Range
from lstore.page import Page
from lstore.config import *
from lstore.file_access import *
from collections import OrderedDict


class Cache:

        def __init__(self):
                # self.db_name = db_name
                self.cache_size = CACHE_SIZE
                self.cache = OrderedDict()

        def __contains(self, key):
                return key in self.cache
        
        def __num_pages_in_cache(self):
                return len(self.cache)

        def __evict(self):
                evict_key = None
                # Looks for oldest unpinned page
                for key in iter(self.cache):
                        if self.cache[key].pinned is False:
                                evict_key = key
                                break

                # If we found an unpinned page to evict
                if evict_key != None:
                        evicted_page = self.cache[evict_key]
                        del self.cache[evict_key]

                        if evicted_page.page_dirty == True:
                                self.__write_disk(evicted_page)
                else:
                        print('Cache is deadlocked!')

        # Write tail pages of page to disk, after merge
        # def evict_tail_pages(self, key):
        #         page_range = self.cache[key]
        #         for i in range(NUM_TAILS_BEFORE_MERGE):
        #                 for j in range(page_range.all_columns):
        #                         filename = 'ECS165a/' + page_range.table_name + '/page_range' + str(page_range.page_range_index) + '/tail/column' + str(j)
        #                         file_offset = i
        #                         if os.path.exists(filename):
        #                                 file_offset = os.path.getsize(filename) // (PAGE_SIZE + 8)

        #                         write_page_to_file(page_range.tail_pages[j][i], filename, file_offset)
                
                
        #         for column in page_range.tail_pages:
        #                 column = column[:NUM_TAILS_BEFORE_MERGE]
        #                 column.extend( NUM_TAILS_BEFORE_MERGE * [Page(page_range.table_name, i, page_range.page_range_index, 'tail', j, False, False, PAGE_SIZE, RECORD_SIZE)] )
                        
        #         print('evicted first 3 tails pages')
                                         

        def __read_disk(self, table: Table, page_range_index, page_type, page_index, column):
                
                page = Page(table.name, page_index, page_range_index, page_type, column, False, False, PAGE_SIZE, RECORD_SIZE)
                filename = 'ECS165a/' + str(table.name) + '/page_range' + str(page_range_index) + '/' + page_type + '/column' + str(column)

                if os.path.exists(filename):
                        file_offset = page_index
                        page = read_page_from_file(filename, file_offset)
                
                return page
        
        def __write_disk(self, page):
                        filename = 'ECS165a/' + page.table_name + '/page_range' + str(page.page_range_index) + '/' + page.page_type + '/column' + str(page.column_index)
                        write_page_to_file(page, filename, page.page_index)

        def __insert(self, key, page_range):
                # If cache is full, evict oldest unpinned page
                if self.__num_pages_in_cache() == CACHE_SIZE:
                        self.__evict()
                
                # If cache is no longer full, start insertion process
                if self.__num_pages_in_cache() < CACHE_SIZE:
                        self.cache[key] = page_range

                else: # All pages in cache are pinned, can't insert a new page into cache
                        print('Error inserting new page into the bufferpool/cache')
                        print('Cache is full and all pages are pinned')

        # Pass in a table name and page index, and True if the page will be written to
        def get_physical_page(self, table: Table, page_range_index, page_type, page_index, column, write = False):
                #print('num pages in cache:', self.__num_pages_in_cache())

                # Construct key from table name and page index
                key = (str(table.name) + '/page_range' + str(page_range_index) + '/' + page_type + '/column' + str(column), page_index)
                #print(key, key in self.cache)

                # Check if page being accessed is already in cache
                # Grab the page, take it out of cache and reinsert
                # to make it the most recent item in cache 
                if key in self.cache:
                        page = self.cache[key]
                        del self.cache[key]
                        self.cache[key] = page
                # Grab the page from disk
                else:
                        page = self.__read_disk(table, page_range_index, page_type, page_index, column)
                        self.__insert(key, page)
                
                # Pin page so it isn't evicted while still being accessed
                self.cache[key].pinned = True

                # If write flag is set, 
                if write:
                        self.cache[key].page_dirty = True

                return self.cache[key]
        
        # !!! Only close cache when no queries running !!!
        def close_cache(self):
                while self.cache: # As long as cache isn't empty, pop and write each page to disk
                        page= self.cache.popitem(False)[1] # False means pop oldest item
                        if page.page_dirty == True:
                                self.__write_disk(page)
