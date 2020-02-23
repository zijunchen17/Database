from lstore.table import Table
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
            self._save_table_schema(table)
        pass

    """
    # Creates a new table (user end)
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key):
        ## key = primary key (unique)
        table_dir = self._get_table_dir(name)
        if not os.path.isdir(table_dir):
            os.mkdir(table_dir)
        table_schema = {'name': table_dir,
                        'num_columns': num_columns,
                        'key_column': key,
                        'page_directory': {}}
        table = Table(**table_schema)
        return table


    def get_table(self, name):
        table_metadata_dir = self._get_table_schema_path(name)
        with open(table_metadata_dir, 'r') as f:
            table_schema = json.load(f)

        table = Table(**table_schema)
        self.tables.append(table)
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
        with open(table_schema_path, 'wb') as f:
            json.dump(table_schema, f)
