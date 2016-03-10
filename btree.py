from collections import Mapping, MutableMapping
from sortedcontainers import SortedDict
from msgpack import packb, unpackb
from encode import *

import drawtree


class Tree(MutableMapping):
    def __init__(self, max_size=1024):
        self.root = self._create_leaf(tree=self)
        self.max_size = max_size

    def commit(self):
        self.root._commit()

    @staticmethod
    def _create_leaf(*args, **kwargs):
        return Leaf(*args, **kwargs)

    @staticmethod
    def _create_node(*args, **kwargs):
        return Node(*args, **kwargs)

    def _create_root(self, lhs, rhs):
        root = self._create_node(tree=self)
        root.rest = lhs
        root.bucket[min(rhs.bucket)] = rhs

        return root

    def __getitem__(self, key):
        return self.root[key]

    def __setitem__(self, key, value):
        """
        Inserts the key and value into the root node. If the node has been
        split, creates a new root node with pointers to this node and the new
        node that resulted from splitting.
        """
        result = self.root._insert(key, value)

        if result is None:
            return

        key, value = result
        root = Node(self, True)
        root.rest = self.root
        root.values[key] = value

        self.root = root

    def __delitem__(self, key):
        raise NotImplementedError

    def __iter__(self):
        for key in self.root:
            yield key

    def __len__(self):
        return len(self.root)

class BaseNode(object):
    def __init__(self, tree, changed=False):
        self.tree = tree
        self.changed = changed
        self.bucket = SortedDict()

    def _split(self):
        """
        Creates a new node of the same type and splits the contents of the
        bucket into two parts of an equal size. The lower keys are being stored
        in the bucket of the current node. The higher keys are being stored in
        the bucket of the new node. Afterwards, the new node is being returned.
        """

        other = Node(self.tree, True)
        values = self.values.items()
        self.values = SortedDict(values[:len(values // 2)])
        other.values = SortedDict(values[len(values) // 2:])

        key, value = other.values.popitem(last=False)
        other.rest = value

        return (key, other)

    def _insert(self, key, value):
        """
        Inserts the key and value into the bucket. If the bucket has become too
        large, the node will be split into two nodes.
        """

        self.values[key] = value
        self.changed = True

        if len(self.values) < self.tree.max_size:
            return None

        return self._split()

class Node(BaseNode):
    def __init__(self, *args, **kwargs):
        self.rest = None

        super(Node, self).__init__(*args, **kwargs)

    def _select(self, key):
        """
        Selects the bucket the key should belong to.
        """
        print(self.tree.root)
        print(self.bucket)

        print(min(self.bucket))
        print(self.bucket.values())

        if key < min(self.bucket):
            print('first if')
            new_node = self.rest
            return new_node
        elif key >= max(self.bucket):
            print('second if')
            new_node = self.bucket.values()[-1]
            return new_node

        print('loop')

        for i in range(0, len(self.bucket.keys())-1):
            if key >= self.bucket.keys()[i] and key < self.bucket.keys()[i + 1]:
                new_node = self.bucket.values()[i]
                return new_node

        pass

    def _insert(self, key, value):
        """
        Recursively inserts the key and value by selecting the bucket the key
        should belong to, and inserting the key and value into that back. If the
        node has been split, it inserts the key of the newly created node into
        the bucket of this node.
        """

        result = self._select(key)._insert(key, value)
        self.changed = True

        if result is None:
            return

        key, other = result
        return super()._insert(key, other)

    def _commit(self):
        self.rest._commit()

        for child in self.values.values():
            child._commit()

        data = packb({
            'rest': self.rest.offset,
            'values': {k: v.offset for k, v in self.values.items()},
        })

        self.tree.chunk.write(ChunkId.node, data)
        return self.tree.chunk.tell()

    def __getitem__(self, key):
        return self._select(key)[key]

class Leaf(Mapping, BaseNode):
    def __getitem__(self, key):
        return self.values[key]

    def __iter__(self):
        for key in self.values:
            yield key

    def __len__(self):
        return len(self.values)

    def _split(self):
        other = Node(self.tree, True)

        values = self.values.items()
        self.values = SortedDict(values[:len(values) // 2])
        other.values = SortedDict(values[len(values) // 2:])

        return (min(other.values), other)

    def _commit(self):
        data = packb({
            'values': self.values,
        })

        self.tree.chunk.write(ChunkId.Leaf, data)
        return self.tree.chunk.tell()

class LazyNode(object):
    _init = False

    def __init__(self, offset=None, node=None):
        """
        Sets up a proxy wrapper for a node at a certain disk offset.
        """
        self.offset = offset
        self.node = node
        self._init = True

    @property
    def changed(self):
        """
        Checks if the node has been changed.
        """
        if self.node is None:
            return False

        return self.node.changed

    def _commit(self):
        """
        Commit the changes if the node has been changed.
        """
        if not self.changed:
            return

        self.node._commit()
        self.changed = False

    def _load(self):
        """
        Load the node from disk.
        """
        pass

    def __getattr__(self, name):
        """
        Loads the node if it hasn't been loaded yet, and dispatches the request
        to the node.
        """
        if not self.node:
            self.node = self._load()

        return getattr(self.node, name)

    def __setattr__(self, name, value):
        """
        Dispatches the request to the node, if the proxy wrapper has been fully
        set up.
        """
        if not self._init or hasattr(self, name):
            return super().__setattr__(name, value)

        setattr(self.node, name, value)

def main():
    mytree = Tree()

    mytree.__setitem__(1, "one")

    #drawtree.printTree(mytree)

    #base = BaseNode(mytree);
    #node = Node(mytree)
    #node._insert(4, "two")

    #print(node._insert(6, "Six"))


    print("Hello World!")

if __name__ == '__main__':
    main()