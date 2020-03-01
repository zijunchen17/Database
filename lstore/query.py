from lstore.table import Table, Record
from lstore.index import Index
from time import time



class Query:
    """
    Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        self.index = Index(self.table)
        pass

    def delete(self, key):
        self.table.delete(key)

    def insert(self, *columns):
        """
        Insert a record with specified columns
        """
        schema_encoding = int('0' * self.table.num_columns)
        timestamp = int(time())
        self.index.add_record_to_index(columns,self.table.base_rid)
        # for col_num, val in enumerate(columns):
        #     if self.index.has_index(col_num):
        #         self.index.add_to_index(col_num,val,self.table.base_rid)
        self.table.insert(self.table.key_column, schema_encoding, timestamp, *columns)

    def select(self, key, key_column, query_columns):
        """
        Read a record with specified key
        param key: The key of the record to be update (column in the table)
        column: boolean object with values for the specified columns and None for the rest
        """
        if not self.index.has_index(key_column):
            self.index.create_index(key_column)

        matching_rids = self.index.locate(key_column,key)
        output = []
        for rid in matching_rids:
            output.extend(self.table.select(rid, query_columns, True))
        return output

    def update(self, key, *columns):
        """
        # Update a record with specified key and columns
        """
        timestamp = int(time())
        self.table.update(key, timestamp, *columns)

    def sum(self, start_range, end_range, aggregate_column_index):
        """
        param start_range: int         # Start of the key range to aggregate 
        param end_range: int           # End of the key range to aggregate 
        param aggregate_columns: int   # Index of desired column to aggregate
        """
        return self.table.sum(start_range, end_range, aggregate_column_index)
    
    def print(self):
        page_range = self.table.bufferpool.get_page_range(self.table, 0)
        page_range.print_page_range()

