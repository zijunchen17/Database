import os

from config import *
from page import Page

def read_pages_from_file(all_columns, filename):
    total_bytes = os.path.getsize(filename)
    total_pages = total_bytes // PAGE_SIZE
    total_pages_per_column = total_pages // all_columns
    pages = [[Page() for _ in range(total_pages_per_column)] for _ in range(all_columns)]
    with open(filename, 'rb') as f:
    	for page_num in range(total_pages):
    		c, r = page_num // all_columns, page_num % all_columns
    		pages[r][c].data = f.read(PAGE_SIZE)

    return pages

def write_pages_to_file(pages, filename):
    with open(filename, 'wb') as f:
        for column in pages:
            for page in column:
                f.write(page.data)

# def write_table_schema(num_columns, key_colums):
	

if __name__ == '__main__':
	pages = [[Page() for _ in range(1)] for _ in range(9)]
	for c in pages:
		for page in c:
			page.data = b'1234' * 1024

	write_pages_to_file(pages, 'test')
	pages_read_out = read_pages_from_file(9, 'test')

	print(pages[0][0].data == pages_read_out[0][0].data)
