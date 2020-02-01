from template.page import *
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        # rid: physical (#,offset)
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key_column):
        self.name = name
        self.key_column = key_column + 4
        self.num_columns = num_columns

        self.all_columns = num_columns + 4
        self.base_rid = 1
        self.tail_rid = (1 << 64 - self.all_columns) - 1
        self.page_directory = {}

        self.base_pages = [[Page()] for _ in range(self.all_columns)]
        self.tail_pages = [[] for _ in range(self.all_columns)]
        pass

    def insert(self, schema_encoding, timestamp, *columns):

        rid = self.base_rid
        pages = []

        if not self.base_pages[0][-1].has_capacity():
            for page_list in self.base_pages:
                page_list.append(Page())
        rows = self.base_pages[0][-1].num_records
        rid = (rid << self.all_columns) + rows

        indirection_page = self.base_pages[INDIRECTION_COLUMN][-1].write(0)
        self.base_pages[RID_COLUMN][-1].write(rid)
        self.base_pages[TIMESTAMP_COLUMN][-1].write(timestamp)
        self.base_pages[SCHEMA_ENCODING_COLUMN][-1].write(schema_encoding)
        for i, column in enumerate(columns):
            self.base_pages[i+4][-1].write(column)

        for page_list in self.base_pages:
            pages.append(page_list[-1])

        self.page_directory[rid] = pages

        self.base_rid += 1

    def __update(self):
        pass


    def __merge(self):
        pass

    """
    read a record with specified RID
    """
    def __read(self, rid, query_columns):
        pass
 
