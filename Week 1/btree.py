from collections import Mapping, MutableMapping
from sortedcontainers import SortedDict


class Tree(MutableMapping):

    def __init__(self, max_size=1024):
        self.root = self._create_leaf(tree=self)
        self.max_size = max_size

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

        if hasattr(rhs, "rest"):
            rhs.node.rest = rhs.bucket[min(rhs.bucket)]
            rhs.node.bucket.pop(min(rhs.bucket))

        root.changed = True

        return root

    def __getitem__(self, key):
        return self.root.__getitem__(key)

    def __setitem__(self, key, value):
        """
        Inserts the key and value into the root node. If the node has been
        split, creates a new root node with pointers to this node and the new
        node that resulted from splitting.
        """
        #value = write_document(self.filename, value)

        result = self.root._insert(key, value)

        if result is None:
            return

        key, value = result
        root = Node(self, changed=True)

        print(type(root))

        root.rest = self.root
        root.bucket[key] = value

        self.root = root

    def __delitem__(self, key):
        pass

    def __iter__(self):
        for key in self.root:
            yield key

    def __len__(self):
        return len(self.root)

    def _commit(self):
        pass


class BaseNode(object):

    def __init__(self, tree, changed = False):
        self.tree = tree
        self.bucket = SortedDict()
        self.changed = changed

    def _insert(self, key, value):
        """
        Inserts the key and value into the bucket. If the bucket has become too
        large, the node will be split into two nodes.
        """

        self.bucket[key] = value
        self.changed = True

        if len(self.bucket) < self.tree.max_size:
            return None

        return self._split()

    def _commit(self):
        pass


class Node(BaseNode):

    def __init__(self, *args, **kwargs):
        self.rest = None

        super(Node, self).__init__(*args, **kwargs)

    def _select(self, key):
        """
        Selects the bucket the key should belong to.
        """

        if key < min(self.bucket):
            new_node = self.rest
            return new_node

        elif key >= max(self.bucket):
            new_node = self.bucket.values()[-1]
            return new_node

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

        result = self._select(key)._insert(key,value)
        self.changed = True

        if result is None:
            return

        key, other = result
        return super()._insert(key, other)

    def _split(self):
        """
        Creates a new node of the same type and splits the contents of the
        bucket into two parts of an equal size. The lower keys are being stored
        in the bucket of the current node. The higher keys are being stored in
        the bucket of the new node. Afterwards, the new node is being returned.
        """
        other = self.__class__(tree=self.tree)
        size = len(self.bucket)

        values = self.bucket.items()

        self.bucket = SortedDict(values[:len(values) // 2])
        other.values = SortedDict(values[len(values) // 2:])

        key, value = other.values.popitem(last=False)
        other.rest = value

        return (key, other)

    def __getitem__(self, key):
        selected_node = self._select(key)
        return selected_node.__getitem__(key)

    def __iter__(self):
        if self.rest != None:
            for key in self.rest:
                yield key

        for child in self.bucket.values():
            for key in child:
                yield key

    def __len__(self):
        return sum([len(child) for child in self.buckt.values()])+len(self.rest)


class Leaf(Mapping, BaseNode):

    def _split(self):
        other = Leaf(self.tree)

        values = self.bucket.items()

        self.values = SortedDict(values[:len(values) // 2])
        other.values = SortedDict(values[len(values) // 2:])

        return (min(other.values), other)

    def __getitem__(self, key):

        print('hi')
        print (self.bucket)
        return self.bucket[key]


    def __iter__(self):
        for key in self.bucket:
            yield key


    def __len__(self):
        return len(self.bucket)

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


def printTree(tree):

    import queue as q

    print("-- Tree --")

    queue = q.Queue()

    depth = 0
    prevdepth = -1

    atcurrentdepth = 1
    atdepth = [1,0]
    currentdepth = 0;

    queue.put(tree.root)

    while queue.empty() != True:
        node = queue.get()
        atdepth[currentdepth] -= 1
        print (node)
        bucket = node.bucket

        if depth != prevdepth:
            print("--- d=" + str(depth) + " ---")
            prevdepth = depth

        if type(node) is not Leaf and hasattr(node, 'rest') and node.rest != None:
            queue.put(node.rest)
            atdepth[(currentdepth + 1) % 2] += 1

            print(str(bucket) + " - lhs: " +  str(node.rest))
        else:
            print(str(bucket))


        if bucket != None:
            for i in range(0, len(bucket)):
                print(tree.__getitem__(bucket.iloc[i]))
                if type(bucket[bucket.iloc[i]]) != int:
                    queue.put(node.bucket[node.bucket.iloc[i]])
                    atdepth[(currentdepth + 1) % 2] += 1


        if atdepth[currentdepth] == 0:
            depth += 1
            currentdepth = (currentdepth + 1) % 2

def main():

    # Load the tree from disk and perform some tests, like inserting a new key
    # or retrieving a key.

    tree = Tree(4)

    tree["dockey"] = "testdoc"
    tree["foo"] = "this"
    tree["bar"] = "is"
    tree["what"] = "for"
    tree["up"] = "testing"

    tree["test11"] = "bla11"
    tree["test22"] = "bla22"
    tree["test33"] = "bla33"
    tree["test44"] = "bla44"
    tree["test55"] = "bla55"
    tree["test66"] = "bla66"
    tree["test77"] = "bla77"
    tree["test88"] = "bla88"
    tree["test99"] = "bla98"
    tree["test12"] = "bla11"
    tree["test23"] = "bla22"
    tree["test34"] = "bla33"
    tree["test45"] = "bla44"
    tree["test56"] = "bla55"
    tree["test67"] = "bla66"
    tree["test78"] = "bla77"
    tree["test89"] = "bla88"
    tree["test79"] = "bla98"
    tree["test71"] = "bla11"
    tree["test72"] = "bla22"
    tree["test73"] = "bla33"
    tree["test74"] = "bla44"
    tree["test75"] = "bla55"
    tree["test76"] = "bla66"
    tree["test77"] = "bla77"
    tree["test68"] = "bla88"
    tree["test55"] = "bla98"

    tree._commit()

    print("all keys: ", str([key for key in tree]))

    print(tree.__getitem__("dockey"))

    printTree(tree)

    # tree._commit()

    # compaction(tree)
    #print("Get document: ", str(tree.__getitem__(b"Testkey")))

if __name__ == '__main__':
    main()
