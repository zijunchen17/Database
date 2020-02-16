from lstore.page import *
from time import time
from lstore.config import *
import math
import copy

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
        self.bit_shift = int(math.log(PAGE_SIZE // RECORD_SIZE, 2))
        self.all_columns = num_columns + 4
        self.base_rid = 1
        self.tail_rid = (1 << (64 - self.bit_shift)) - 1
        
        self.page_ranges = [Page_Range()]
        self.page_directory = {}
        self.key_directory = {}

        pass

    def insert(self, schema_encoding, timestamp, *columns):

        rid = self.base_rid
        pages = []

        if not self.base_pages[0][-1].has_capacity():
            for page_list in self.base_pages:
                page_list.append(Page())

        ## return the index of row, starting from 0 to 511 (512 rows in total)
        rows = self.base_pages[0][-1].num_records
        rid = (rid << self.bit_shift) + rows

        ## insert values into each column (or each page)
        indirection_page = self.base_pages[INDIRECTION_COLUMN][-1].write(0)
        self.base_pages[RID_COLUMN][-1].write(rid)
        self.base_pages[TIMESTAMP_COLUMN][-1].write(timestamp)
        self.base_pages[SCHEMA_ENCODING_COLUMN][-1].write(schema_encoding)
        for i, column in enumerate(columns):
            self.base_pages[i+4][-1].write(column)

        for page_list in self.base_pages:
            pages.append(page_list[-1])

        ## map each rid to specific pages
        self.page_directory[rid] = pages
        self.key_directory[pages[self.key_column].read(rows)] = rid

        self.base_rid += 1

    def update(self, key, timestamp, *columns):
        pages = []
        
        if not self.tail_pages[0][-1].has_capacity():
            for page_list in self.tail_pages:
                page_list.append(Page())

        ##############################
        # Write to tail page
        ##############################
        
        new_tail_row = self.tail_pages[0][-1].num_records
        tail_rid = (self.tail_rid << self.bit_shift) + new_tail_row

        self.tail_pages[INDIRECTION_COLUMN][-1].write(0)
        self.tail_pages[RID_COLUMN][-1].write(tail_rid)
        self.tail_pages[TIMESTAMP_COLUMN][-1].write(timestamp)
        
        # Create schema encoding and write to columns.
        tail_schema = ''
        for i, col in enumerate(columns):
            if col is not None:
                tail_schema += '1'
                self.tail_pages[i+4][-1].write(col)
            else:
                tail_schema += '0'
                self.tail_pages[i+4][-1].write(SPECIAL_NULL_VALUE)

        # Set tail's schema
        self.tail_pages[SCHEMA_ENCODING_COLUMN][-1].write(int(tail_schema))
        for page_list in self.tail_pages:
            pages.append(page_list[-1])
        self.page_directory[tail_rid] = pages   

        ##############################
        # Associate with base page
        ##############################

        # Fetch base_indirection, base_rid, base_row, base_schema, and page_list
        base_rid = self.key_directory[key]
        base_row = self._get_row(base_rid)
        base_page_list = self.page_directory[base_rid]
        base_indirection_rid = base_page_list[INDIRECTION_COLUMN].read(base_row)
        base_page_schema = base_page_list[SCHEMA_ENCODING_COLUMN].read(base_row)

        # Add tail_rid to base_page if no pre-existing tail pages
        if base_indirection_rid == 0:
            base_page_list[INDIRECTION_COLUMN].write(tail_rid,base_row)
        # Else set last tail_page's indirection pointer to the new tail_page
        else:
            next_tail_rid = base_indirection_rid
            latest_tail_row = self._get_row(next_tail_rid)
            while next_tail_rid:
                next_tail_record = self.page_directory[next_tail_rid]
                next_tail_row = self._get_row(next_tail_rid)
                next_tail_rid = next_tail_record[INDIRECTION_COLUMN].read(next_tail_row)
            next_tail_record[INDIRECTION_COLUMN].write(tail_rid,next_tail_row)
            

        # Update base_page's schema
        tail_schema = int(tail_schema,2)
        new_schema = base_page_schema|tail_schema
        base_page_list[SCHEMA_ENCODING_COLUMN].write(int(new_schema),base_row)
        self.tail_rid -= 1

    def _get_row(self, rid):
        return rid & ((1 << self.bit_shift) - 1)

        """
    read a record with specified RID
    """
    def __read(self,rid,query_columns):
        row = self._get_row(rid)
        page_list = self.page_directory[rid]
        page_list = page_list[SCHEMA_ENCODING_COLUMN+1:]
        page_list = [p for i,p in enumerate(page_list) if query_columns[i] == 1]
        return [page.read(row) for page in page_list]


    ## update latest values 
    def __update_column(self, base_column, tail_column):
        # create boolean list to check which column needs to be updated
        b = [x != SPECIAL_NULL_VALUE for x in tail_column]
        for i,v in enumerate(b):
            if v:
                base_column[i] = tail_column[i]
        return base_column


    ## select the record having the latest values
    def select(self, key, query_columns):

        if key in self.key_directory:
            cur_rid = self.key_directory[key]
            cur_columns = self.__read(cur_rid, query_columns)
            row = self._get_row(cur_rid)
            next_rid = self.page_directory[cur_rid][INDIRECTION_COLUMN].read(row)
            while next_rid:
                cur_rid = copy.deepcopy(next_rid)
                next_row = self._get_row(cur_rid)
                next_columns = self.__read(cur_rid,query_columns)
                cur_columns = self.__update_column(cur_columns, next_columns)
                next_rid = self.page_directory[cur_rid][INDIRECTION_COLUMN].read(next_row)
            return [Record(key, cur_rid, cur_columns)]
        else:
            print('Key {} does not exist!'.format(key))

    ### only read the base directory 
    # def select(self, key, query_columns):
    #     if key in self.key_directory:

    #         # records = []
    #         cur_rid = self.key_directory[key]
    #         row = self._get_row(cur_rid)
    #         page_list = self.page_directory[cur_rid]
    #         page_list = page_list[SCHEMA_ENCODING_COLUMN+1:]
    #         page_list = [p for i,p in enumerate(page_list) if query_columns[i] == 1]
    #         cur_columns = [page.read(row) for page in page_list]
    #         return [Record(key, cur_rid, cur_columns)]
            

    #     else:
    #         print('Key {} does not exist!'.format(key))


    def _key_directory_tail(self,key):
        if key in self.key_directory:
            cur_rid = self.key_directory[key]
            #row = self._get_row(cur_rid)
            next_rid = self.page_directory[cur_rid][INDIRECTION_COLUMN]
            rid_tail_list = []
            while next_rid:
                next_row = self._get_row(next_rid)
                next_rid = self.page_directory[next_rid][INDIRECTION_COLUMN].read(next_row)
                rid_tail_list.append(next_rid) # page_directory name confirmation
            return rid_tail_list
        else:
            print('Key {} does not exist!'.format(key))

    def delete(self, key):
        if key in self.key_directory:
            cur_rid = self.key_directory[key]
            row = self._get_row(cur_rid)
            next_rid = self.page_directory[cur_rid][INDIRECTION_COLUMN].read(row)
            self.page_directory[cur_rid][RID_COLUMN].update(0, row)
            del self.key_directory[key]
            while next_rid:
                next_row = self._get_row(next_rid)
                next_rid_save = self.page_directory[next_rid][RID_COLUMN].read(next_row)
                self.page_directory[next_rid][RID_COLUMN].update(0, next_row)
                next_rid = self.page_directory[next_rid_save][INDIRECTION_COLUMN].read(next_row)
        else:
            print('Key {} does not exist!'.format(key))

    def sum(self, start_range, end_range, aggregate_column_index):

        column_value = []

        query_columns = [0] * self.num_columns
        query_columns[aggregate_column_index] = 1

        for key in range(start_range, end_range+1):

            if key in self.key_directory:
                record_list = self.select(key, query_columns)
                record = record_list[0]
                value = record.columns[0]
                column_value.append(value)

            # else:
                # print('Key {} does not exist!'.format(key))

        return sum(column_value)

    def __merge(self):
        pass


