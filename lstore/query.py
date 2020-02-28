from lstore.table import Table, Record
from lstore.index import Index
from time import time


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table 
    """

    def __init__(self, table):
        self.table = table
        pass

    """
    # internal Method
    # Read a record with specified RID
    """

    def delete(self, key):
        self.table.delete(key)

    """
    # Insert a record with specified columns
    """

    def insert(self, *columns):
        schema_encoding = int('0' * self.table.num_columns)
        timestamp = int(time())
        self.table.insert(schema_encoding, timestamp, *columns)
        pass

    """
    # Read a record with specified key
    # param key: The key of the record to be update (column in the table)
    # colum: boolean object with values for the specified columns and None for the rest
    """

    def select(self, select_index, key, query_columns):

        return self.table.select(key, select_index, query_columns)

    """
    # Update a record with specified key and columns
    """

    def update(self, key, *columns):
        timestamp = int(time())
        self.table.update(key, timestamp, *columns)
        pass

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    """

    def sum(self, start_range, end_range, aggregate_column_index):

        return self.table.sum(start_range, end_range, aggregate_column_index)
    
    def print(self):
        for range in self.table.page_ranges:
            range.print_page_range()
