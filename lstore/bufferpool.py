from lstore.config import *
from lstore.utils import *

class buffer_pool():
    def __init__(self, buffer_size = BUFFERPOOL_SIZE):
        self.buffer_size = BUFFERPOOL_SIZE
        self.mru_list = list(range(10))
        self.dirty_list = [0]*10
        self.page_in_pool = self.open_pool()

    def get_range_index(self, rid):
        range_index = rid // (BASE_PAGES_PER_RANGE * PAGE_SIZE)
        self.update_mru(range_index)
        if self.update_mru.if_evict:
            self.update_bufferpool(range_index)
        else:
            self.shuffle_bufferpool(range_index)
        return range_index

    def update_mru(self,range_index):
        if range_index in self.mru_list:
            self.update_mru.if_evict = False
            self.mru_list.insert(0, self.mru_list.pop(self.mru_list.index(range_index)))
        else:
            self.update_mru.if_evict = True
            self.write_dirty_page()
            self.mru_list = [range_index] + self.mru_list[:-1]

    def write_dirty_page(self): #dirty or new page

    def update_bufferpool(self,range_index):
        read_pages_from_file() #from mru_list
        return cur_bufferpool

    def shuffle_bufferpool(self,range_index):
        return cur_bufferpool

    def open_pool(self):
        return cur_pool

    def close_pool(self):



