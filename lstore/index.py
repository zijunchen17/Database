"""
A data structure holding indices for various columns of a table. Key column
should be indexd by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.
"""
from BTrees import IOBTree

class Index:
    
    def __init__(self, table):
        self.indices = [None] * table.num_columns
        self.table = table
        self.create_index(table.key_column-4)

    def locate(self, column, value):
        ''' Returns the RIDs of all records with provided value in specified column. '''
        if not self.has_index(column):
            return -1
        rids = self.indices[column].get(value)
        return rids

    def locate_range(self, begin, end, column):
        ''' Returns the RIDs of all records with values in column "column" between "begin" and "end"'''
        set_of_vals = list(self.indices[column].values(min=begin,max=end))
        return set_of_vals

    def has_index(self, column_number):
        ''' Returns whether column_number is indexed '''
        return self.indices[column_number] is not None

    def add_record_to_index(self, columns, rid):
        ''' Index columns as needed from new record '''
        for col_num, val in enumerate(columns):
            if self.has_index(col_num):
                self.add_to_index(col_num,val,rid)

    def add_to_index(self, column, value, rid):
        ''' Add existing record to column index '''
        if self.indices[column].has_key(value):
            self.indices[column].get(value).append(rid)
        else:
            self.indices[column][value] = [rid]

    def create_index(self, column_number):
        ''' Create index on specific column '''
        print(f"Creating index on column number {column_number}")
        if not self.has_index(column_number):
            self.indices[column_number] = IOBTree.IOBTree()
            col_selection = [0]*self.table.num_columns
            col_selection[column_number] = 1
            for key in self.table.key_directory:
                table_select_val = self.table.select(key,col_selection)[0].columns[0]
                self.add_to_index(column_number, table_select_val, self.table.key_directory[key])
        else:
            print(f"Index already created for column {column_number}")

    def drop_index(self, column_number):
        ''' Drop index of specific column '''
        self.indicies[column_number] = None
