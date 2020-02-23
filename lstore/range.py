from lstore.page import *
from lstore.config import *
from lstore.utils import *
import math

class Page_Range:

    def __init__(self, all_columns, base_pages=None, tail_pages=None):
        self.full = False
        if base_pages is None:
            base_pages = [[Page()] for _ in range(all_columns)]
        
        if tail_pages is None:
            tail_pages = [[Page()] for _ in range(all_columns)]

        self.base_pages = base_pages
        self.tail_pages = tail_pages
    
    def has_capacity(self):
        if len(self.base_pages[0]) == BASE_PAGES_PER_RANGE and not self.base_pages[0][-1].has_capacity():
            self.full = True

        return not self.full

    def get_base_page(self, index):
        return self.base_pages[index]
    
    def get_base_page_index(self, rid):
        return rid // (PAGE_SIZE // RECORD_SIZE)
    
    def get_last_base_page(self):
        if len(self.base_pages[0]) < BASE_PAGES_PER_RANGE and not self.base_pages[0][-1].has_capacity():
            self.__add_base_page()

        return [column[-1] for column in self.base_pages]
    
    def get_last_base_page_index(self):
        return len(self.base_pages) - 1

    def get_tail_page(self, index):
        return self.tail_pages[index]
    
    def get_last_tail_page(self):
        if not self.tail_pages[0][-1].has_capacity():
            self.__add_tail_page()
        
        return [column[-1] for column in self.tail_pages]

    def __add_base_page(self):
        for page_list in self.base_pages:
            page_list.append(Page())

    def __add_tail_page(self):
        for page_list in self.tail_pages:
            page_list.append(Page())

            ############
            # need to check for merge
            ############
            self.__add_tail_page.check_merge = True

    def write_to_disk(self, page_range_dir):
        base_filename = os.path.join(page_range_dir, 'base')
        tail_filename = os.path.join(page_range_dir, 'tail')
        write_pages_to_file(self.base_pages, base_filename)
        write_pages_to_file(self.tail_pages, tail_filename)

    @classmethod
    def read_from_disk(cls, all_columns, page_range_dir):
        base_filename = os.path.join(page_range_dir, 'base')
        tail_filename = os.path.join(page_range_dir, 'tail')
        base_pages = read_pages_from_file(all_columns, base_filename)
        tail_pages = read_pages_from_file(all_columns, tail_filename)
        return cls(all_columns, base_pages, tail_pages)


# page_range = Page_Range(all_columns)
# page_range.write_to_disk('~/ECS165/1')
# page_range = Page_Range.read_from_disk(all_columns, '~/ECS165/1')

    


        