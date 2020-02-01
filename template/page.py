from template.config import *
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

    def write(self, value):

    	b_value= value.to_bytes(self.record_size,byteorder = BYTE_ORDER)
    	self.data[self.__pos : self.__pos + self.record_size] = b_value
    	self.num_records += 1
        #pass


