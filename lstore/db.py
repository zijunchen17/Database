from lstore.table import Table
from lstore.cache import Cache

class Database():

    def __init__(self):
        self.tables = []
        self.open()
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
    def create_table(self, name, num_columns, key_index):
        ## key = primary key (unique)
        table = Table(self.bufferpool, name, num_columns, key)
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
