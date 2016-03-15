from yamr import Database, Chunk, Tree
from mapreduce_wrapper import Script

'''
a = Script()
a.add_file('map.py')
'''

db = Database('test.db', max_size=4)

for i in range(0, 1000):
    print(i)
    db[i] = 'WTF'

db.commit()
