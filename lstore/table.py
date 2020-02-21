from lstore.page import *
from lstore.range import *
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
        
        self.page_ranges = [Page_Range(self.all_columns)]
        self.page_directory = {}
        self.key_directory = {}

        pass

    def insert(self, schema_encoding, timestamp, *columns):

        rid = self.base_rid
        
        # A new page range is allocated only when the previous ones are full.
        # If the last page range is full, then a new one must be allocated.
        if not self.page_ranges[-1].has_capacity():
            self.page_ranges.append(Page_Range(self.all_columns))
        
        # A new base page is allocated only when the previous ones are full.
        # If the last base page is full, then a new one must be allocated.
        base_page = self.page_ranges[-1].get_last_base_page()
        physical_page_offset = (rid - 1) % (PAGE_SIZE // RECORD_SIZE - 1)

        base_page[INDIRECTION_COLUMN].write(0)
        base_page[RID_COLUMN].write(rid)
        base_page[TIMESTAMP_COLUMN].write(timestamp)
        base_page[SCHEMA_ENCODING_COLUMN].write(schema_encoding)

        for i, column in enumerate(columns):
            base_page[i+4].write(column)
        
        self.page_directory[rid] = base_page
        self.key_directory[base_page[self.key_column].read(physical_page_offset)] = rid
        self.base_rid += 1

    def update(self, key, timestamp, *columns):

        base_rid = self.key_directory[key]
        range_index = base_rid // PAGE_RANGE_SIZE
        
        tail_page = self.page_ranges[range_index].get_last_tail_page()
        tail_physical_page_offset = tail_page[0].num_records
        tail_rid = (self.tail_rid << self.bit_shift) + tail_physical_page_offset

        ##############################
        # Write to tail page
        ##############################

        tail_page[INDIRECTION_COLUMN].write(0)
        tail_page[RID_COLUMN].write(tail_rid)
        tail_page[TIMESTAMP_COLUMN].write(timestamp)

        # Create schema encoding and write to columns.
        tail_schema = ''
        for i, col in enumerate(columns):
            if col is not None:
                tail_schema += '1'
                tail_page[i+4].write(col)
            else:
                tail_schema += '0'
                tail_page[i+4].write(SPECIAL_NULL_VALUE)

        # Set tail's schema
        tail_page[SCHEMA_ENCODING_COLUMN].write(int(tail_schema))
        
        self.page_directory[tail_rid] = tail_page


        ##############################
        # Associate with base page
        ##############################

        # Fetch base_indirection, base_row, base_schema, and page_list
        base_page = self.page_directory[base_rid]
        base_physical_page_offset = (base_rid - 1) % (PAGE_SIZE // RECORD_SIZE - 1)
        base_indirection_rid = base_page[INDIRECTION_COLUMN].read(base_physical_page_offset)
        base_page_schema = base_page[SCHEMA_ENCODING_COLUMN].read(base_physical_page_offset)

        # If base_indirection is not 0 (has existing tail records), 
        # set base_indirection to rid of new tail record
        #if base_indirection_rid != 0:
        latest_tail_rid = base_indirection_rid
        #print(latest_tail_rid)
        tail_page[INDIRECTION_COLUMN].write(latest_tail_rid, tail_physical_page_offset)

        # Add tail_rid to base_page's indirection
        base_page[INDIRECTION_COLUMN].write(tail_rid, base_physical_page_offset)


        # Update base_page's schema
        tail_schema = int(tail_schema,2)
        new_base_schema = base_page_schema|tail_schema
        base_page[SCHEMA_ENCODING_COLUMN].write(int(new_base_schema), base_physical_page_offset)
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
            base_rid = self.key_directory[key]
            base_page = self.page_directory[base_rid]
            base_physical_page_offset = (base_rid - 1) % (PAGE_SIZE // RECORD_SIZE - 1)
            base_schema = base_page[SCHEMA_ENCODING_COLUMN].read(base_physical_page_offset)
            base_schema = format(base_schema, "b")
            base_schema = '0' * (self.num_columns - len(base_schema)) + base_schema
            

            cur_columns = [None] * self.num_columns

            for i, col in enumerate(base_schema):
                if col == '0' and query_columns[i] == 1:
                    cur_columns[i] = base_page[i+4].read(base_physical_page_offset)
            
            tail_rid = base_page[INDIRECTION_COLUMN].read(base_physical_page_offset)


            while int(base_schema,2) & int(''.join(str(col) for col in query_columns), 2) != 0:
                tail_page = self.page_directory[tail_rid]
                tail_physical_page_offset = self._get_row(tail_rid)
                tail_schema = tail_page[SCHEMA_ENCODING_COLUMN].read(tail_physical_page_offset)
                tail_schema = str(tail_schema)
                tail_schema = '0' * (self.num_columns - len(tail_schema)) + tail_schema
                

                for i, col in enumerate(tail_schema):
                    if base_schema[i] == tail_schema[i] == '1' and query_columns[i] == 1:
                        cur_columns[i] = tail_page[i+4].read(tail_physical_page_offset)
                        base_schema = base_schema[:i] + '0' + base_schema[i+1:]
                
                tail_rid = tail_page[INDIRECTION_COLUMN].read(tail_physical_page_offset)
            
            filtered_columns = filter(self.__remove_none, cur_columns)
            
            cur_columns = []
            for column in filtered_columns:
                cur_columns.append(int(column))

            return [Record(key, base_rid, cur_columns)]
        else:
            print('Key {} does not exist!'.format(key))

    def __remove_none(self, x):
        if x == None:
            return False
        else:
            return True


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
            base_rid = self.key_directory[key]
            base_page = self.page_directory[base_rid]
            base_physical_page_offset = (base_rid -1 ) % (PAGE_SIZE // RECORD_SIZE - 1)
            base_page[RID_COLUMN].write(0, base_physical_page_offset)
            next_rid = base_page[INDIRECTION_COLUMN].read(base_physical_page_offset)

            del self.key_directory[key]
            del self.page_directory[base_rid]

            while next_rid:
                tail_page = self.page_directory[next_rid]
                tail_page[RID_COLUMN].write(0, self._get_row(next_rid))
                del self.page_directory[next_rid]
                next_rid = tail_page[INDIRECTION_COLUMN].read(self._get_row(next_rid))
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


