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

tail_schema = '0110011'
base_page_schema = '0000000'
tail_schema = int(tail_schema,2)
base_page_schema = int(base_page_schema,2)
new_base_schema = base_page_schema|tail_schema
print(bin(new_base_schema).count('1'))



d = [[1, '34', '44'], [1, '40', '30', '41'], [1, '41', '40', '42'], [1, '42', '41', '43'], [1, '43', '42', '44'], [1, '44', '34', '43']]
print(list(set().union(*d)))