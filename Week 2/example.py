#!env/bin/python

"""
Author: S.J.R. van Schaik <stephan@synkhronix.com>
Example usage of the basic yamr module.
"""

from yamr import Database, Chunk, Tree

db = Database('test.db', max_size=4)

db[3] = 'foo'
db[6] = 'bar'
db[1] = 'test'
db[7] = 'this'
db[9] = 'that'
db[10] = 'that'
db[13] = 'that'
db[16] = 'that'
db[19] = 'that'
db[30] = 'that'
db[21] = 'that'
db[80] = 'that'

print(db[3])

for k, v in db.items():
    print('{}: {}'.format(k, v))

db.commit()