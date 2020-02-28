import os
from lstore.config import *
from lstore.page import Page
from lstore.range import Page_Range



#### write bases pages to file
#### Note: every time, page_range.basepages evicts/reads the whole base_pages
#### in bufferpool, bases_pages overwrite 
#### during merge, base_pages append 
def write_basepages_to_file(base_pages, filename, all_columns, file_offset = None):
	## overwrite base pages file
	if not os.path.isfile(filename):
		file_offset = 0
	seek_pos = file_offset * BASE_PAGES_PER_RANGE * (PAGE_SIZE+8) * all_columns
	with open(filename, "wb") as f:
		f.seek(seek_pos)
		for p in range(BASE_PAGES_PER_RANGE):
			for c in range(all_columns):
				page = base_pages[c][p]
				f.write(page.data)
				f.write(page.num_records.to_bytes(8, byteorder = BYTE_ORDER))
	return file_offset

def merge_write_basepages_to_file(base_pages, filename, all_columns, file_offset):
	## append base pages
	## update page_directory
	if os.path.isfile(filename):
		with open(filename, 'ab') as f:
			for p in range(BASE_PAGES_PER_RANGE):
				for c in range(all_columns):
					page = base_pages[c][p]
					f.write(page.data)
	return file_offset + 1


def read_basepages_from_file(filename,all_columns, file_offset):
	## read base pages(last positions)
	# if os.path.isfile(filename):
	# 	file_offset = os.path.getsize(filename) // (BASE_PAGES_PER_RANGE * PAGE_SIZE * all_columns)
	# 	file_offset = file_offset - 1
	seek_pos = file_offset * BASE_PAGES_PER_RANGE * (PAGE_SIZE + 8) * all_columns
	base_pages = [[Page() for _ in range(BASE_PAGES_PER_RANGE)] for _ in range(all_columns)]
	if os.path.isfile(filename):
		with open(filename, 'rb') as f:
			f.seek(seek_pos)
			for p in range(BASE_PAGES_PER_RANGE):
				for c in range(all_columns):
					base_pages[c][p].data = f.read(PAGE_SIZE)
					base_pages[c][p].num_records = int.from_bytes(f.read(8), BYTE_ORDER)
	return base_pages

#### tail pages:
####
####
####

def write_tailpages_to_file(tail_pages, filename, TAIL_SIZE, all_columns):
	if os.path.isfile(filename):
		seek_pos = os.path.getsize(filename)
	else:
		seek_pos = 0

	with open(filename, "wb") as f:
		f.seek(seek_pos)
		for p in range(TAIL_SIZE):
			for c in range(all_columns):
				page = tail_pages[c][p]
				f.write(page.data)

def read_last_3tailpages_from_file(filename, TAIL_SIZE, all_columns):
	seek_pos = _seek_tail_page_pos(filename, TAIL_SIZE, all_columns)
	tail_pages = [[Page() for _ in range(TAIL_SIZE)] for _ in range(all_columns)]
	with open(filename, "rb") as f:
		f.seek(seek_pos)
		for p in range(TAIL_SIZE):
			for c in range(all_columns):
				tail_pages[c][p].data = f.read(PAGE_SIZE)
	return tail_pages


def _seek_tail_page_pos(filename, TAIL_SIZE, all_columns):
	if os.path.isfile(filename):
		total_bytes = os.path.getsize(filename)
		total_pages = total_bytes // PAGE_SIZE
		total_pages_per_column = total_pages // all_columns
		seek_pos = (total_pages_per_column - TAIL_SIZE) * all_columns * PAGE_SIZE
	else:
		seek_pos = 0
	return seek_pos

def _get_file_name(table_name, range_index, page_type):
	page_dir = os.path.join(table_name, str(range_index))
	if not os.path.isdir(page_dir):
		os.makedirs(page_dir)
	return os.path.join(page_dir, page_type)


def _update_page_directory(table, range_index, file_offset):
    filepath = _get_filepath(table.name, range_index, BASE_PAGE_TYPE)
    table.page_dirtory[(range_index)] = filepath, file_offset

# if __name__ == '__main__':

# 	pages = [[Page() for _ in range(8)] for _ in range(9)]
# 	temp1  = b'1111' * 1024
# 	temp2 = b'2222' * 1024
# 	temp3 = b'3333' * 1024
# 	temp4  = b'4444' * 1024
# 	temp5 = b'5555' * 1024
# 	temp6 = b'6666' * 1024
# 	temp7 = b'7777' * 1024
# 	temp8 = b'8888' * 512 + b'\x00\x00'* 512
# 	temps = [[temp1, temp2, temp3, temp4, temp5, temp6, temp7, temp8] for _ in range(9)]
# 	for p in range(8):
# 		for c in range(9):
# 			# print("data in column" ,c , "page index is ", p)
# 			# for i in range(512):
# 			# 	pages[c][p].write(temps[c][p][i*8 : (i+1)*8],i)s
# 			pages[c][p].data = temps[c][p]
# 			pages[c][p].num_records = 111
# 			# print(pages[c][p].data)
# 			# print(temps[c][p])
# 			# print(pages[c][p].data)



# 	# print(len(pages[0]))
# 	# tails = [[Page() for _ in range(3)] for _ in range(9)]
# 	# temp6 = b'6666' * 1024
# 	# temp7 = b'7777' * 1024
# 	# temp8 = bytearray(4096)
# 	# tempss = [[temp6, temp7, temp8] for _ in range(9)]
# 	# for p in range(3):
# 	# 	for c in range(9):
# 	# 		tails[c][p].data = tempss[c][p]

# 	# for p in range(3):
# 	# 	for c in range(9):
# 	# 		print("data in column" ,c , "page index is ", p)
# 	# 		print(pages[c][p].data)

# 	filename = _get_file_name('/Users/yatingge/ECS165/Grades', 3 , "base")
# 	offset1 = write_basepages_to_file(pages, filename, 9, 0)

# 	base_pages = read_basepages_from_file(filename, 9, offset1)
# 	print(base_pages[8][7].num_records)


	
# 	# print(base_pages[8][7].data)

# 	# filename2 = _get_file_name('/Users/yatingge/ECS165/Grades', 1 , "tail")
# 	# write_tailpages_to_file(tails, filename2, 3, 9)
# 	# tail_pages = read_tailpages_from_file(filename2, 3, 9)









