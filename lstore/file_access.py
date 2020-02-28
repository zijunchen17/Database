import os

from lstore.config import *
from lstore.utils import parse_page_attributes_from_filename
from lstore.page import Page

### write a single physical page() from disk
### if page() does not exist: append to the file and get the offset of file
### else page() exists: overwrite the file to corresponding positions
def write_page_to_file(page, filename, file_offset = None):
    if file_offset is None:
        #### if file exists, we could get the file size
        #### otherwise starts from 0
        if os.path.isfile(filename):
            file_offset = os.path.getsize(filename)//(PAGE_SIZE + RECORD_SIZE)
        else:
            file_offset = 0
    
    seek_pos = file_offset * (PAGE_SIZE + RECORD_SIZE)
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    if os.path.exists(filename):
        with open(filename, 'rb+') as f:
            f.seek(seek_pos)
            f.write(page.data)
            f.write(page.num_records.to_bytes(RECORD_SIZE,byteorder = BYTE_ORDER))
    else:
        with open(filename, 'wb') as f:
            f.seek(seek_pos)
            f.write(page.data)
            f.write(page.num_records.to_bytes(RECORD_SIZE,byteorder = BYTE_ORDER))

    return file_offset


#### read a single physical page() from file based on the offset
#### return a page() object
def read_page_from_file(filename, file_offset):
    ### read page attributes
    temp, column_index = os.path.split(filename)
    temp, page_type = os.path.split(temp)
    temp, page_range_index = os.path.split(temp)
    attributes = parse_page_attributes_from_filename(filename)
    page = Page(attributes[0], file_offset, attributes[1], attributes[2], attributes[3])

    with open(filename, 'rb') as f:
        seek_pos = file_offset * (PAGE_SIZE + RECORD_SIZE)
        f.seek(seek_pos)
        page.data = bytearray(f.read(PAGE_SIZE))
        page.num_records = int.from_bytes(f.read(8), BYTE_ORDER)
    return page


#### given table_name and page(), returns the filename corresponding to the page
def _get_filepath(table_name, page):
    page_dir = os.path.join(table_name, str(page.page_range_index), str(page.page_type))
    if not os.path.isdir(page_dir):
        os.makedirs(page_dir)
    return os.path.join(page_dir, str(page.column_index))

##### store below info to page_directory
##### update : when bufferpool.evict
##### write to disk: when db.close()


# The key of bufferpool is rid and column_index. Its value is a page object.
def _update_page_directory(table, rid, file_offset, page):
    filepath = _get_filepath(table.name, page)
    table.page_dirtory[(rid, page.column_index)] = filepath, file_offset



####################################################page_range granuality###################################
# def read_pages_from_file(all_columns, filename):
#     total_bytes = os.path.getsize(filename)
#     total_pages = total_bytes // PAGE_SIZE
#     total_pages_per_column = total_pages // all_columns
#     pages = [[Page() for _ in range(total_pages_per_column)] for _ in range(all_columns)]
#     with open(filename, 'rb') as f:
#         for page_num in range(total_pages):
#             c, r = page_num // all_columns, page_num % all_columns
#             pages[r][c].data = f.read(PAGE_SIZE)

#     return pages

# def write_pages_to_file(pages, filename):
#     with open(filename, 'wb') as f:
#         for column in pages:
#             for page in column:
#                 f.write(page.data)


# if __name__ == '__main__':
# 	# pages = [[Page() for _ in range(3)] for _ in range(9)]
# 	# for c in pages:
# 	# 	for page in c:
# 	# 		page.data = b'1234' * 1024
#  #    write_pages_to_file(pages, 'test')
#  #    pages_read_out = read_pages_from_file(9, 'test')

#  #    print(pages[0][0].data == pages_read_out[0][0].data)


#     page = Page(5,'tail',6)
#     page.data = b'7777' * 1024
#     filename = _get_filepath('/Users/yatingge/ECS165/Grades/', page)
#     offset = write_page_to_file(page, filename, None)
#     page1 = read_page_from_file(filename,offset)
#     print(page.data == page1.data, page1.page_range_index, page1.page_type, page1.column_index)


	