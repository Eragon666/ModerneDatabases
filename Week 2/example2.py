"""
Author: S.J.R. van Schaik <stephan@synkhronix.com>
Example usage of the basic yamr module.
"""

from yamr import Database, Chunk, Tree

db = Database('test.db', max_size=4)

db[11] = 'foo'
db[13] = 'bar'
db[17] = 'test'
db[19] = 'this'
db[23] = 'that'

print(db[3])

for k, v in db.items():
    print('{}: {}'.format(k, v))

db.commit()
db.close()