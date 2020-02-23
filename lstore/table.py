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

    def __str__(self):
        return str(self.columns)

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

        if not self.page_ranges[-1].has_capacity():
            self.page_ranges.append(Page_Range(self.all_columns))
        
        base_page = self.page_ranges[-1].get_last_base_page()

        physical_page_offset = rid % (PAGE_SIZE // RECORD_SIZE)

        base_page[INDIRECTION_COLUMN].write(0)
        base_page[RID_COLUMN].write(rid)
        base_page[TIMESTAMP_COLUMN].write(timestamp)
        base_page[SCHEMA_ENCODING_COLUMN].write(schema_encoding)

        for i, column in enumerate(columns):
            base_page[i+4].write(column, physical_page_offset)
        
        self.page_directory[rid] = base_page
        self.key_directory[base_page[self.key_column].read(physical_page_offset)] = rid
        # print(base_page[self.key_column].read(physical_page_offset))
        self.base_rid += 1

    def update(self, key, timestamp, *columns):
        pages = []

        base_rid = self.key_directory[key]
        range_index = base_rid // PAGE_RANGE_SIZE
        
        tail_page = self.page_ranges[range_index].get_last_tail_page()
        tail_physical_page_offset = tail_page[0].num_records
        tail_rid = (self.tail_rid << self.bit_shift) + tail_physical_page_offset
        ##############################
        # merge if needed
        ##############################

        self.__merge()
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
        base_physical_page_offset = base_rid % (PAGE_SIZE // RECORD_SIZE)
        base_indirection_rid = base_page[INDIRECTION_COLUMN].read(base_physical_page_offset)
        base_page_schema = base_page[SCHEMA_ENCODING_COLUMN].read(base_physical_page_offset)

        # If base_indirection is not 0 (has existing tail records), 
        # set base_indirection to rid of new tail record
        if base_indirection_rid != 0:
            latest_tail_rid = base_indirection_rid
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

    def __merge(self, range_index):
        if self.page_ranges[range_index].__add_tail_page().check_merge:
            tail_len = len(self.page_ranges[range_index].tail_pages[0][:-1])
            if tail_len >= MERGE_SIZE and tail_len % MERGE_SIZE == 0:
                ########
                # trig merge
                ########
                tails_to_merge = [each_page[-MERGE_SIZE-1:-1] for each_page in self.page_ranges[range_index].tail_pages]
                base_copy = copy.deepcopy(self.page_ranges[range_index].base_pages[SCHEMA_ENCODING_COLUMN+1:])


                ########
                # change merge trigger back
                ########
                self.page_ranges[range_index].__add_tail_page().check_merge = False
            else:
                self.page_ranges[range_index].__add_tail_page().check_merge = False
            pass
        else:
            pass

    @ staticmethod
    def _get_location_record(baserid_list, base_rid):
        """
        find rows of records with the same BaseRID in one set of tail pages
        """
        record_location = [i for i, x in enumerate(baserid_list) if x == base_rid]
        return record_location

    @staticmethod
    def _latest_per_rid_per_col(record_location, per_col_per_tail):
        """
        get latest record in one tail page for a column
        """
        updates_each_rid = [per_col_per_tail.read(row) for row in record_location][::-1] # from latest to oldest
        ltst_col_rcd = next((x for i, x in enumerate(updates_each_rid) if x != SPECIAL_NULL_VALUE), SPECIAL_NULL_VALUE)
        return ltst_col_rcd

    def _latest_per_rid_per_tail(self, record_location, all_col_per_tail):
        """
        get the latest record in one set of tail page for all columns
        """
        ltst_rcds_per_tail = [self._latest_per_rid_per_col(record_location, per_tail) for per_tail in all_col_per_tail]
        return ltst_rcds_per_tail

    @staticmethod
    def update_counts(base_schema):
        """
        how many columns has been updated
        """
        return bin(base_schema).count('1')

    def _latest_all_rids_per_tail(self, record_locations, tails_to_merge, base_schema_encode, ltst_rcds_all_tails):
        """
        give a rid, get the latest update for all column through all tail pages
        """
        for record_locations, per_tail in zip(record_location_list, tails_to_merge):


    @staticmethod
    def baserid_in_per_tail(baserid_one_tail):
        baserid_list = [baserid_one_tail.read(row) for row in range(PAGE_SIZE // RECORD_SIZE)]
        return baserid_list

    def baserid_in_all_tails(self, baserid_all_tails):
       baserid_list_list  = [self.baserid_in_per_tail(baserid_one_tail) for baserid_one_tail in baserid_all_tails]
       baserid_all = list(set().union(*baserid_list_list))
       return baserid_all


        record_location_list = [self._get_location_record(baserid_list, base_rid) for base_rid in baserid_set]




















