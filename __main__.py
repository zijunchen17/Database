from lstore.db import Database
from lstore.query import Query
from lstore.utils import print_page_range
from time import process_time
from random import choice, randrange

# Student Id and 4 grades
db = Database()
db.open('~/ECS165')
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)
keys = []

# Measuring Insert Performance
insert_time_0 = process_time()
for i in range(0, 10000):
    query.insert(906659671 + i, 93, 0, 0, 0)
    keys.append(906659671 + i)
insert_time_1 = process_time()
print("Inserting 10k records took:  \t\t\t", insert_time_1 - insert_time_0)



# Measuring update Performance
update_cols = [
    [None, None, None, None, None],
    [None, randrange(0, 100), None, None, None],
    [None, None, randrange(0, 100), None, None],
    [None, None, None, randrange(0, 100), None],
    [None, None, None, None, randrange(0, 100)],
]
# query.print()

update_time_0 = process_time()
for i in range(0, 10000):
    query.update(choice(keys), *(choice(update_cols)))
    #print_page_range(grades_table, 0)
    #print_page_range(grades_table, 1)
update_time_1 = process_time()
print("Updating 10k records took:  \t\t\t", update_time_1 - update_time_0)
print_page_range(grades_table, 0)
print_page_range(grades_table, 1)
# Measuring Select Performance
select_time_0 = process_time()
for i in range(0, 10000):
    query.select(choice(keys), 0, [1, 1, 1, 1, 1])
select_time_1 = process_time()
print("Selecting 10k records took:  \t\t\t", select_time_1 - select_time_0)

# Measuring Aggregate Performance
agg_time_0 = process_time()
#for i in range(0, 10000, 100):
result = query.sum(906659671, 906659671 + 10000, 1)
agg_time_1 = process_time()
print("Aggregate 10k of 100 record batch took:\t", agg_time_1 - agg_time_0)

# # Measuring Delete Performance
delete_time_0 = process_time()
for i in range(0, 10000):
    query.delete(906659671 + i)
delete_time_1 = process_time()
print("Deleting 10k records took:  \t\t\t", delete_time_1 - delete_time_0)



