a = [1, 2, 3, 4, 5]
b = a.copy()
b.append(8)
print(b)
print(a)

a = (1024).to_bytes(2, byteorder='big')
b = (1025).to_bytes(2, byteorder='big')
print(a == b)

a = [1,2,3,3,4,4,5,6,]
print(a[::-1])

print(int('011001', 10))

tail_schema = '011001100101'
base_page_schema = '000000000000'
tail_schema = int(tail_schema,2)
base_page_schema = int(base_page_schema,2)
new_base_schema = base_page_schema|tail_schema
test = bin(new_base_schema)[1:]
print([int(x == '1') for i, x in enumerate(test)])

d = [[1, '34', '44'], [1, '40', '30', '41'], [1, '41', '40', '42'], [1, '42', '41', '43'], [1, '43', '42', '44'], [1, '44', '34', '43']]
print(list(set().union(*d)))

baserid_list = [1,2,3,4,5,5,2,3,1,4]
print([i for i, x in enumerate(baserid_list) if x == 7])

def test(x):
    return 2+x
a = [1,2,3]
print([test(x) for x in a])


def _latest_per_rid_all_cols(self, base_rid, tails_to_merge, baserid_in_all_tails):
    [self._latest_per_rid_per_col(base_rid, tails_one_col, baserid_in_all_tails) for tails_one_col in tails_to_merge]

a = [1,2,3,4,5,6]
print(a[-4::])

print(len([]*10))