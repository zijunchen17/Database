"""
A data strucutre holding indices for various columns of a table. Key column
should be indexd by default, other columns can be indexed through this object.
Indices are usually B-Trees, but other data structures can be used as well.
"""
from BTrees import IOBTree

class Index:
    
    def __init__(self, table):
        self.indices = [None] * table.num_columns
        self.table = table
        self.create_index(table.key_column)

    def locate(self, column, value):
        '''
        ###Returns the location of all records with the given value on column "column"
        Returns the RIDs of all records with provided value in specified column.
        '''
        if not has_index(column):
            print(f"Column {column} hasn't been indexed")
            return -1
        vals = self.indices[column].get(value)
        print(vals)
        return vals

    def locate_range(self, begin, end, column):
        '''
        Returns the RIDs of all records with values in column "column" between "begin" and "end"
        '''
        set_of_vals = list(self.indices[column].values(min=begin,max=end))
        print(set_of_vals)
        return set_of_vals

    def has_index(self, column_number):
        return self.indices[column_number] is not None

    def add_to_index(self, column, value, rid):
        if self.indices[column].has_key(value):
            self.indices[column].get(value).append(rid)
        else:
            self.indices[column][value] = [rid]

    def create_index(self, column_number):
        '''
        Create index on specific column
        '''
        if not self.has_index(column_number):
            self.indices[column_number] = IOBTree.IOBTree()
            col_selector = [0]*self.table.num_columns
            col_selector[column_number] = 1
            for key in self.table.key_directory:
                self.add_to_index(column_number, self.table.select(key,col_selector)[0], self.table.key_directory[key])
        else:
            print(f"Index already created for column {column_number}")

    def drop_index(self, column_number):
        '''
        Drop index of specific column
        '''
        self.indicies[column_number] = None
