from lstore.config import *
import math
class Page:

    def __init__(self, page_size = PAGE_SIZE, record_size = RECORD_SIZE):

        self.record_size = RECORD_SIZE
        self.page_size = PAGE_SIZE
        self.num_records = 0
        self.data = bytearray(self.page_size)
    
    @property  
    def __pos(self):
        return self.num_records * self.record_size

    def has_capacity(self):
    	"""
    	"""
    	return self.__pos + self.record_size <= self.page_size
      	#pass 

    def write(self, value, row = None):
        
        b_value= value.to_bytes(self.record_size,byteorder = BYTE_ORDER)

        if row is not None:
            self.data[row * self.record_size:(row+1) * self.record_size] = b_value
        else:
            self.data[self.__pos : self.__pos + self.record_size] = b_value
            self.num_records += 1
        
        
        #pass

    def read(self, row):
        b_value = self.data[row * self.record_size : (row+1) * self.record_size]
        return int.from_bytes(b_value, byteorder = BYTE_ORDER)

    # new function
    def update(self, value_update, row):
        b_value = value_update.to_bytes(self.record_size, byteorder=BYTE_ORDER)
        self.data[row * self.record_size:(row+1) * self.record_size] = b_value
