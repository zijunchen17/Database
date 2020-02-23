from table import Table
from index import Index
import os

class Database():

    def __init__(self):
        self.tables = []
        pass

    def open(self):

        pass

    def close(self):
        pass

    """
    # Creates a new table (user end)
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        ## key = primary key (unique)
        table = Table(name, num_columns, key)
        return table


    def get_table(self, name):
        pass

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass
