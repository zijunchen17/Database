from lstore.table import Table
from lstore.cache import Cache
import os
import json

class Database():

    def __init__(self):
        self.tables = []
        self.db_name = '&&'
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        self.db_dir = os.path.join(parent_dir, 'ECS165a')
        if not os.path.isdir(self.db_dir):
            os.mkdir(self.db_dir)
        pass

    def open(self, db_name):
        self.db_name = db_name
        self.bufferpool = Cache()
        pass

    def close(self):
        for table in self.tables:
            self._save_table_schema(table)
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
        table_dir = self._get_table_dir(name)
        if not os.path.isdir(table_dir):
            os.mkdir(table_dir)
        table_schema = {'name': name,
                        'num_columns': num_columns,
                        'key_index': key_index,
                        'key_directory': {}}
        table = Table(self.bufferpool, **table_schema)
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """

    def get_table(self, name):
        table_metadata_dir = self._get_table_schema_path(name)
        with open(table_metadata_dir, 'r') as f:
            table_schema = json.load(f)
        table = Table(self.bufferpool, **table_schema)
        self.tables.append(table)
        return table

    def drop_table(self, name):
        pass

    def _get_table_dir(self, name):
        return os.path.join(self.db_dir, name)

    def _get_table_schema_path(self, name):
        return os.path.join(self._get_table_dir(name), 'table_schema')

    def _save_table_schema(self, table):
        table_schema = table.get_table_schema()
        table_schema_path = self._get_table_schema_path(table.name)
        with open(table_schema_path, 'w') as f:
            json.dump(table_schema, f)
