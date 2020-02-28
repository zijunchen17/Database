from lstore.table import Table
from lstore.cache import Cache

class Database():

    def __init__(self):
        self.tables = []
        self.bufferpool = None
        pass

    def open(self):
        self.bufferpool = Cache()
        pass

    def close(self):
        self.bufferpool.close_cache()
        pass

    """
    # Creates a new table (user end)
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        ## key = primary key (unique)
        table = Table(self.bufferpool, name, num_columns, key)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass
