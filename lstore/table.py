from lstore.page import *
from lstore.range import *
from time import time
from lstore.config import *
from lstore.utils import *
from lstore.lock import readWriteLock
import math
import copy
import threading

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3
BASE_TPS_COLUMN = -1 # Only in base pages
TAIL_BASE_RID_COLUMN = -1 # Only in tail pages


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
    def __init__(self, bufferpool, name, num_columns, key_index, key_directory, tail_page_directory, tail_page_index_directory, base_rid = 1, tail_rid = 2**64 - 2):

        self.bufferpool = bufferpool
        self.name = name
        self.key_index = key_index
        self.key_column = key_index + 4
        self.num_columns = num_columns
        self.bit_shift = int(math.log(PAGE_SIZE // RECORD_SIZE, 2))
        self.all_columns = num_columns + 5
        self.base_rid = base_rid
        self.tail_rid = tail_rid
        
        self.tail_page_directory = tail_page_directory # tail_rid -> (page_range_index, page_index, tail_physical_offset)
        str_tail_page_directory = self.tail_page_directory
        self.tail_page_directory = {int(c[0]):c[1] for c in str_tail_page_directory.items()}

        self.tail_page_index_directory = tail_page_index_directory # page_range_index -> latest_tail_page_index
        self.key_directory = key_directory

        str_key_directory = key_directory
        self.key_directory = {int(c[0]):int(c[1]) for c in str_key_directory.items()}
        self.page_range_locks = {}
        self.flag = False

        pass

    def insert(self, key_column, schema_encoding, timestamp, *columns):

        rid = self.base_rid

        page_range_index = get_page_range_index(rid)

        # Acquire a lock so merge doesn't swap original and copy base pages
        # while this query is running
        if page_range_index not in self.page_range_locks:
            new_lock = readWriteLock()
            self.page_range_locks[page_range_index] = new_lock
        
        while not self.page_range_locks[page_range_index].acquire_write():
            pass

        base_page_index = get_base_page_index(rid)
        base_physical_page_offset = get_base_physical_offset(rid)

        base_page = [ _ for _ in range(self.all_columns)]

        base_page[INDIRECTION_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, INDIRECTION_COLUMN, write=True)
        base_page[INDIRECTION_COLUMN].write(0)
        base_page[RID_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, RID_COLUMN, write=True)
        base_page[RID_COLUMN].write(rid)
        base_page[TIMESTAMP_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, TIMESTAMP_COLUMN, write=True)
        base_page[TIMESTAMP_COLUMN].write(timestamp)
        base_page[SCHEMA_ENCODING_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, SCHEMA_ENCODING_COLUMN, write=True)
        base_page[SCHEMA_ENCODING_COLUMN].write(schema_encoding)
        base_page[BASE_TPS_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, self.all_columns - 1, write=True)
        base_page[BASE_TPS_COLUMN].write(0)

        for i, column in enumerate(columns):
            base_page[i+4] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, i+4, write=True)
            base_page[i+4].write(column)
        
        #self.page_directory[rid] = base_page
        self.key_directory[base_page[self.key_column].read(base_physical_page_offset)] = rid
        
        self.base_rid += 1
        
        for column in base_page:
            column.pinned -= 1
        
        self.page_range_locks[page_range_index].release_write()


    def update(self, key, timestamp, *columns):

        base_rid = self.key_directory[key]

        page_range_index = get_page_range_index(rid)

        # Acquire a lock so merge doesn't swap original and copy base pages
        # while this query is running
        if page_range_index not in self.page_range_locks:
            new_lock = readWriteLock()
            self.page_range_locks[page_range_index] = new_lock
        
        while not self.page_range_locks[page_range_index].acquire_write():
            pass
        
        base_page_index = get_base_page_index(base_rid)
        base_physical_page_offset = get_base_physical_offset(base_rid)

        # Grab base page, no need to grab value columns
        base_page = [ _ for _ in range(0, self.all_columns - self.num_columns)]
        base_page[INDIRECTION_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, INDIRECTION_COLUMN, write=True)
        base_page[RID_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, RID_COLUMN)
        base_page[TIMESTAMP_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, TIMESTAMP_COLUMN)
        base_page[SCHEMA_ENCODING_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, SCHEMA_ENCODING_COLUMN, write=True)
        base_page[BASE_TPS_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, self.all_columns - 1, write=True)

        
        tail_rid = self.tail_rid

        # Check if the page range has any tail pages
        if page_range_index not in self.tail_page_index_directory:
            self.tail_page_index_directory[page_range_index] = 0

        # Find the latest tail page of the page range
        tail_page_index = self.tail_page_index_directory[page_range_index]
        tail_page = [ _ for _ in range(self.all_columns)]

        # Grab latest tail page to check if it's full
        tail_page[RID_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'tail', tail_page_index, RID_COLUMN)

        # Check if latest tail page is full, if it is, increment the latest tail page index to point to a newly allocated one
        if not tail_page[RID_COLUMN].has_capacity():
            self.tail_page_index_directory[page_range_index] += 1
            # trigger merge
            tail_page_index = self.tail_page_index_directory[page_range_index]
        # Unpin the page we used to check if the latest tail page is full
        tail_page[RID_COLUMN].pinned -= 1 
        
        # Grab the tail page in which the new tail record will be put into
        # (Either a brand new tail page or the existing one that isn't full)
        for column in range(self.all_columns):   
            tail_page[column] = self.bufferpool.get_physical_page(self, page_range_index, 'tail', tail_page_index, column, write=True)


        tail_physical_page_offset = tail_page[RID_COLUMN].num_records

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

        ##############################
        # Associate with base page
        ##############################

        # Fetch base_indirection, base_row, base_schema, and page_list
        base_indirection_rid = base_page[INDIRECTION_COLUMN].read(base_physical_page_offset)
        base_schema = base_page[SCHEMA_ENCODING_COLUMN].read(base_physical_page_offset)

        # Point the tail record to its base record (write to base_rid column)
        tail_page[self.all_columns - 1].write(base_rid, tail_physical_page_offset)

        # Set indirection of the new tail record to point to the most recent (last) tail record
        # If the new tail record is the first tail record, then its indirection will be set to 0
        latest_tail_rid = base_indirection_rid
        tail_page[INDIRECTION_COLUMN].write(latest_tail_rid, tail_physical_page_offset)

        # Add tail_rid to base_page's indirection
        base_page[INDIRECTION_COLUMN].write(tail_rid, base_physical_page_offset)


        # Update base_page's schema
        base_schema = int(str(base_schema), 2)
        tail_schema = int(tail_schema,2)
        new_base_schema = format(base_schema|tail_schema, "b")
        base_page[SCHEMA_ENCODING_COLUMN].write(int(new_base_schema), base_physical_page_offset)
        
        # Add tail page to page directory
        self.tail_page_directory[tail_rid] = (page_range_index, tail_page_index, tail_physical_page_offset)

        self.tail_rid -= 1



        # if tail_page_index == NUM_TAILS_BEFORE_MERGE - 1 and tail_physical_page_offset == PAGE_SIZE // RECORD_SIZE - 1:
        #     print('merge start')
        #     #page_range.print_page_range()
        #     page_range.merging = True
        #     # print('tail page index:',tail_page_index, 'tail physical offset:', tail_physical_page_offset )
        #     self.__merge(page_range)
        #     # x = threading.Thread(target=self.__merge, args=(page_range,))
        #     # x.start()
        #     self.flag = True
        
        for column in base_page:
            column.pinned -= 1
        for column in tail_page:
            column.pinned -= 1

        self.page_range_locks[page_range_index].release_write()


  
        
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


    def quick_select(self, rid, query_columns):
        # TODO: Don't need to check key directory. Should also be able to 
        # go straight to the page needed. Other todo in query.select().
        pass

        

    ## select the record having the latest values
    def select(self, key, query_columns, rid_provided = False):
        #rid is passed in instead from query select using index
        # for j in range(0, PAGE_SIZE // RECORD_SIZE):
        #     for k in range(table.all_columns):
        #         physical_page = table.bufferpool.get_physical_page(table, 0, 'base', 0, k)
        #         print(physical_page.read(j),end=' ')
        #         physical_page.pinned = False
        #     print('\n',end='')
        
        if key in self.key_directory or rid_provided:
            if not rid_provided:
                base_rid = self.key_directory[key]
            else:
                base_rid = key
            
            page_range_index = get_page_range_index(rid)

            # Acquire a lock so merge doesn't swap original and copy base pages
            # while this query is running
            if page_range_index not in self.page_range_locks:
                new_lock = readWriteLock()
                self.page_range_locks[page_range_index] = new_lock
            
            while not self.page_range_locks[page_range_index].acquire_write():
                pass

            base_page_index = get_base_page_index(base_rid)
            base_physical_page_offset = get_base_physical_offset(base_rid)

            base_page = [ _ for _ in range(self.all_columns)]
            base_page[INDIRECTION_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, INDIRECTION_COLUMN)
            base_page[RID_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, RID_COLUMN)
            base_page[TIMESTAMP_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, TIMESTAMP_COLUMN)
            base_page[SCHEMA_ENCODING_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, SCHEMA_ENCODING_COLUMN)
            base_page[BASE_TPS_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, self.all_columns - 1)

            for i in range(0, self.num_columns):
                base_page[i+4] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, i+4)

            base_schema = base_page[SCHEMA_ENCODING_COLUMN].read(base_physical_page_offset)
            # page_range.print_page_range()
            # print("base_rid", base_rid)
            # print("fetched:",base_schema)
            # if self.flag:
            #     import pdb; pdb.set_trace()
            # print("first schema:", base_schema)
            base_schema = format(base_schema, f"0{self.num_columns}")
            # base_schema = '0' * (self.num_columns - len(base_schema)) + base_schema
            
            cur_columns = [None] * self.num_columns

            # print(query_columns)
            # print(base_schema)
            # print(len(base_schema))
            for i, col in enumerate(base_schema):
                if col == '0' and query_columns[i] == 1:
                    base_page[i+4] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, i+4)
                    cur_columns[i] = base_page[i+4].read(base_physical_page_offset)
            
            
            tail_rid = base_page[INDIRECTION_COLUMN].read(base_physical_page_offset)


            while int(base_schema,2) & int(''.join(str(col) for col in query_columns), 2) != 0:
                #print('key:', key , 'base_schema:', base_schema)
                #print('hello')
                tail_page_index = self.tail_page_directory[tail_rid][1]
                tail_physical_page_offset = self.tail_page_directory[tail_rid][2]
                tail_page = [ _ for _ in range(self.all_columns)]
                for column in range(self.all_columns):
                    tail_page[column] = self.bufferpool.get_physical_page(self, page_range_index, 'tail', tail_page_index, column)

                tail_schema = tail_page[SCHEMA_ENCODING_COLUMN].read(tail_physical_page_offset)
                tail_schema = str(tail_schema)
                tail_schema = '0' * (self.num_columns - len(tail_schema)) + tail_schema
                

                for i, col in enumerate(tail_schema):
                    if base_schema[i] == tail_schema[i] == '1' and query_columns[i] == 1:
                        cur_columns[i] = tail_page[i+4].read(tail_physical_page_offset)
                        base_schema = base_schema[:i] + '0' + base_schema[i+1:]
                
                tail_rid = tail_page[INDIRECTION_COLUMN].read(tail_physical_page_offset)
                for column in tail_page:
                    column.pinned -= 1
            
            filtered_columns = filter(self.__remove_none, cur_columns)
            
            cur_columns = []
            for column in filtered_columns:
                cur_columns.append(int(column))

            for column in base_page:
                column.pinned -= 1
            
            self.page_range_locks[page_range_index].release_write()

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

            page_range_index = get_page_range_index(rid)

            # Acquire a lock so merge doesn't swap original and copy base pages
            # while this query is running
            if page_range_index not in self.page_range_locks:
                new_lock = readWriteLock()
                self.page_range_locks[page_range_index] = new_lock
            
            while not self.page_range_locks[page_range_index].acquire_write():
                pass

            base_page_index = get_base_page_index(base_rid)
            base_physical_page_offset = get_base_physical_offset(base_rid)
            
            base_page = [ _ for _ in range(2)]

            base_page[INDIRECTION_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, INDIRECTION_COLUMN)
            base_page[RID_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'base', base_page_index, RID_COLUMN, write=True)
            base_page[RID_COLUMN].write(0, base_physical_page_offset)
            next_rid = base_page[INDIRECTION_COLUMN].read(base_physical_page_offset)

            for column in base_page:
                column.pinned -= 1

            del self.key_directory[key]

            while next_rid:
                tail_rid = next_rid
                tail_page_index = self.tail_page_directory[tail_rid][1]
                tail_physical_page_offset = self.tail_page_directory[tail_rid][2]

                tail_page = [ _ for _ in range(2)]
                
                tail_page[INDIRECTION_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'tail', tail_page_index, INDIRECTION_COLUMN)
                tail_page[RID_COLUMN] = self.bufferpool.get_physical_page(self, page_range_index, 'tail', tail_page_index, RID_COLUMN, write=True)

                tail_page[RID_COLUMN].write(0, tail_physical_page_offset)
                next_rid = tail_page[INDIRECTION_COLUMN].read(tail_physical_page_offset)

                for column in tail_page:
                    column.pinned -= 1

                del self.tail_page_directory[tail_rid]

            self.page_range_locks[page_range_index].release_write()
        else:
            print('Key {} does not exist!'.format(key))

    def sum(self, start_range, end_range, aggregate_column_index):

        column_value = []

        query_columns = [0] * self.num_columns
        query_columns[aggregate_column_index] = 1

        for key in range(start_range, end_range+1):

            if key in self.key_directory:
                select_index = 0
                record_list = self.select(key, query_columns)
                record = record_list[0]
                value = record.columns[0]
                column_value.append(value)

            # else:
                # print('Key {} does not exist!'.format(key))

        return sum(column_value)

    def get_table_schema(self):
        table_schema = {'name': self.name,
                        'num_columns': self.num_columns,
                        'key_index': self.key_index,
                        'key_directory': self.key_directory,
                        'tail_page_directory': self.tail_page_directory,
                        'tail_page_index_directory': self.tail_page_index_directory,
                        'base_rid': self.base_rid,
                        'tail_rid': self.tail_rid}
        return table_schema

    def __merge(self, page_range_index, tail_page_index):

        base_pages = [ [] for _ in range(self.all_columns)]
        for i in range(BASE_PAGES_PER_RANGE):
                        for j in range(self.all_columns):
                            base_pages[j][i] = self.bufferpool.get_physical_page(self, page_range_index, 'base', i, j)

        tail_page = [ _ for _ in range(self.all_columns)]
        for j in range(self.all_columns):
            tail_page[j] = self.bufferpool.get_physical_page(self, page_range_index, 'tail', tail_page_index, j)

        tails_to_merge = tail_page[SCHEMA_ENCODING_COLUMN + 1 : self.all_columns]
        
        base_copy = copy.deepcopy(base_pages)
        base_copy = self.merge_in_process(base_copy, tails_to_merge)
        while not self.page_range_locks[page_range_index].acquire_write():
            pass
        # replace base original with base copy

        self.page_range_locks[page_range_index].release_write()

    @staticmethod
    def _get_location_record(baserid_list, base_rid):
        """
        find rows of records with the same BaseRID in one set of tail pages
        """
        record_location = [i for i, x in enumerate(baserid_list) if x == base_rid]
        return record_location

    @staticmethod
    def _latest_per_rid_per_col_per_tail(record_location, per_col_per_tail):
        """
        get latest record in one tail page for a column which schema_ecode is 1
        """
        updates_each_rid = [per_col_per_tail.read(row) for row in record_location][::-1]  # from latest to oldest
        ltst_col_rcd = next((x for i, x in enumerate(updates_each_rid) if x != SPECIAL_NULL_VALUE), SPECIAL_NULL_VALUE)
        return ltst_col_rcd

    def _latest_per_rid_per_col(self, base_rid, tails_one_col, baserid_in_all_tails):
        """
        get the latest record in all tail pages for one column which schema_encode is 1
        """
        updated_retrieved = SPECIAL_NULL_VALUE
        for i in range(NUM_TAILS_BEFORE_MERGE):
            baserid_list = baserid_in_all_tails[i]
            record_location = self._get_location_record(baserid_list, base_rid)
            if len(record_location) != 0:
                updated_retrieved = self._latest_per_rid_per_col_per_tail(record_location, tails_one_col[i])
                if updated_retrieved != SPECIAL_NULL_VALUE:
                    return updated_retrieved
        return updated_retrieved

    @staticmethod
    def schema_vector(base_schema):
        """
        return vector like [0,1,1,0...]
        """
        get_bin = lambda x: format(x, '05b')
        base_schema = get_bin(base_schema)
        return [int(x == '1') for i, x in enumerate(base_schema)]

    @staticmethod
    def get_basepage_index(rid):
        return (rid - 1) // (PAGE_SIZE // RECORD_SIZE)

    @staticmethod
    def get_base_row(rid):
        return (rid - 1) % (PAGE_SIZE // RECORD_SIZE)

    @staticmethod
    def baserid_in_per_tail(baserid_one_tail):
        baserid_list = [baserid_one_tail.read(row) for row in range(PAGE_SIZE // RECORD_SIZE)]
        return baserid_list

    def baserid_in_all_tails(self, baserid_all_tails):
        baserid_list_list = [self.baserid_in_per_tail(baserid_one_tail) for baserid_one_tail in baserid_all_tails]
        baserid_all = set().union(*baserid_list_list)
        return baserid_all, baserid_list_list

    def merge_in_process(self, base_copy, tails_to_merge):
        baserid_all, baserid_list_list = self.baserid_in_all_tails(tails_to_merge[TAIL_BASE_RID_COLUMN])
        baserid_all.discard(0)
        for base_rid in baserid_all:
            base_page_index = self.get_basepage_index(base_rid)
            base_row = self.get_base_row(base_rid)
            base_schema = base_copy[SCHEMA_ENCODING_COLUMN][base_page_index].read(base_row)
            base_vector = self.schema_vector(base_schema)
            tails_to_merge_ext = tails_to_merge[-1 - self.num_columns:-1]
            #######
            # go merge
            #######
            latest_update = []
            for i in range(self.num_columns):
                if base_vector[i]:
                    ltst_rcd_pe_col = self._latest_per_rid_per_col(base_rid, tails_to_merge_ext[i],
                                                                   baserid_list_list)
                    latest_update.append(ltst_rcd_pe_col)
                else:
                    latest_update.append(SPECIAL_NULL_VALUE)
            ###########
            # over write base copy
            ###########
            for i, col in enumerate(latest_update):
                if col != SPECIAL_NULL_VALUE:
                    base_copy[i + 4][base_page_index].write(col, base_row)
            base_copy[SCHEMA_ENCODING_COLUMN][base_page_index].write(int('0' * self.num_columns, 2), base_row)
            base_copy[INDIRECTION_COLUMN][base_page_index].write(0, base_row)
        return base_copy


    #### close/open table without bufferpool
    #### write all base pages and tail pages into the disk files
    def close(self):
        for page_range in self.page_ranges:
            self.__write_page_range(page_range, BASE_PAGE_TYPE)
            self.__write_page_range(page_range, TAIL_PAGE_TYPE)
    
    ### write page range back to file
    def __write_page_range(self, page_range, page_type):
        pages = eval("page_range." + page_type + "_pages") 
        all_rids = self.__read__all_rids(page_range, page_type)
        for c in range(self.all_columns):
            for i, page in enumerate(pages[c]):
                # if len(all_rids[i]) != 0:
                filename = get_filepath(self.name, page)
                offset = write_page_to_file(page, filename, None)
                update_file_directory(self, all_rids[i], offset, page)
        

    ### read all rids for each page range
    def __read__all_rids(self, page_range, page_type):
        all_rids = []
        pages = eval("page_range." + page_type + "_pages")
        for page in pages[RID_COLUMN]:
            all_rids.append(self._read_one_page(page))
        return all_rids

    ### read one whole/entie page 
    def _read_one_page(self, page):
        result = []
        for i in range(page.num_records):
            result.append(page.read(i))
        return result



