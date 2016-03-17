"""
Author: S.J.R. van Schaik <stephan@synkhronix.com>

Reference implementation of the database abstraction for yet another map-reduce
database (yamr-db).
"""

from collections import MutableMapping
from msgpack import packb, unpackb

from .btree import LazyNode, Tree
from .chunk import Chunk, ChunkId

import sys
import os


class Database(MutableMapping):

    def __init__(self, path, max_size=1024):
        data = None
        offset = None
        size = 0

        try:
            with open(path, 'rb+') as f:
                chunk = Chunk(f)

                try:
                    while chunk.verify():
                        if chunk.get_id() == ChunkId.Commit:
                            offset = chunk.tell()
                            chunk.next()
                            size = chunk.tell()
                        else:
                            chunk.next()
                except EOFError:
                    pass

                chunk.f.truncate(size)

                if offset is not None:
                    chunk.seek(offset)
                    data = unpackb(chunk.read())
        except FileNotFoundError:
            pass

        self.chunk = Chunk(open(path, 'ab+'))

        if data is None:
            self.tree = Tree(self.chunk, max_size)
            return

        self.tree = Tree(self.chunk, data[b'max_size'])
        self.tree.root = LazyNode(offset=data[b'documents'], tree=self.tree)

    def commit(self):
        self.tree.commit()

        data = packb({
            'documents': self.tree.root.offset,
            'max_size': self.tree.max_size,
        })

        self.chunk.write(ChunkId.Commit, data)
        self.chunk.flush()

    def close(self):
        self.chunk.close()

    def __getitem__(self, key):
        return self.tree[key]

    def __setitem__(self, key, value):
        self.tree[key] = value

    def __delitem__(self, key):
        del self.tree[key]

    def __len__(self):
        return len(self.tree)

    def __iter__(self):
        yield from self.tree

    # Compact database
    def compaction(self):
        leaf_list = self.__iter__()

        # Open temporary database
        temp_chunk = Chunk(open('temp.db', 'ab+'))

        # Create temporary tree
        temp_tree = Tree(temp_chunk, self.tree.max_size)

        # Use iterator to get key values, store key and value in temporary
        #   tree
        while 1:
            try:
                key = leaf_list.__next__()
                value = self.tree.__getitem__(key)
                temp_tree.__setitem__(key, value)
            except:
                break

        self.tree = temp_tree
        self.chunk = temp_chunk

        self.commit()

        # Replace old database with temporary database
        os.rename('temp.db', 'test.db')
