from lstore.table import Table
from lstore.range import Page_Range
from lstore.config import *
from lstore.utils import *
import os
import json


class Database():

    def __init__(self):
        self.tables = []
        pass

    def open(self,db_dir):
        self.db_dir = db_dir
        pass

    def close(self):
        for table in self.tables:
            table.close()
            self._save_table_schema(table)
        pass

    """
    # Creates a new table (user end)
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        ## key = primary key (unique)
        table_dir = self._get_table_dir(name)
        if not os.path.isdir(table_dir):
            os.mkdir(table_dir)
        table_schema = {'name': table_dir,
                        'num_columns': num_columns,
                        'key_index': key_index,
                        'file_directory': {}}
        table = Table(**table_schema)
        self.tables.append(table)
        return table


    def get_table(self, name):
        table_metadata_dir = self._get_table_schema_path(name)
        with open(table_metadata_dir, 'r') as f:
            table_schema = json.load(f)
        table = Table(**table_schema)
        self.tables.append(table)

        base_pages_list, tail_pages_list = self._read_table_from_file(table)
        table = self._covert_to_page_range(table, base_pages_list, tail_pages_list)
        return table

    ### read all base pages and all tail pages
    def _read_table_from_file(self,table):
        base_pages_list = []
        tail_pages_list = []
        for key, value in sorted(table.file_directory.items()):
            key = eval(key)
            if key[4] == BASE_PAGE_TYPE:
                page = read_page_from_file(value[0], value[1], key[2])
                base_pages_list.append(page)
            else:
                page = read_page_from_file(value[0], value[1], key[2])
                tail_pages_list.append(page)

        return base_pages_list, tail_pages_list

    def _covert_to_page_range(self, table, base_pages_list, tail_pages_list):
        
        num_page_ranges = len(base_pages_list) // (BASE_PAGES_PER_RANGE * table.all_columns) + 1
        table.page_ranges = [Page_Range(i, table.all_columns) for i in range(num_page_ranges)]

        # for p in base_pages_list:
        #     # print(p.page_range_index, p.column_index, p.page_index)
        #     # print(table.page_ranges[p.page_range_index].base_pages[p.column_index])
        #     table.page_ranges[p.page_range_index].base_pages[p.column_index][p.page_index -1] = p

        for p in base_pages_list:
            cur_base_len = len(table.page_ranges[p.page_range_index].tail_pages[p.column_index])
            if cur_base_len < p.page_index:
                for _ in range(p.page_index - cur_base_len):
                    table.page_ranges[p.page_range_index].add_base_page()
            table.page_ranges[p.page_range_index].base_pages[p.column_index][p.page_index -1] = p

        for p in tail_pages_list:
            cur_tail_len = len(table.page_ranges[p.page_range_index].tail_pages[p.column_index])
            if cur_tail_len < p.page_index:
                for _ in range(p.page_index-cur_tail_len):
                    table.page_ranges[p.page_range_index].add_tail_page()
            table.page_ranges[p.page_range_index].tail_pages[p.column_index][p.page_index- 1] = p

        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass

    def _get_table_dir(self, name):
        return os.path.join(os.path.expanduser(self.db_dir), name)

    def _get_table_schema_path(self, name):
        return os.path.join(self._get_table_dir(name), 'table_schema')

    def _save_table_schema(self, table):
        table_schema = table.get_table_schema()
        table_schema_path = self._get_table_schema_path(table.name)
        with open(table_schema_path, 'w') as f:
            json.dump(table_schema, f)
