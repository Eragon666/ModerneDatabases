
def printTree(tree):

    import queue as q
    import tmp

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

        bucket = node.bucket

        if depth != prevdepth:
            print("--- d=" + str(depth) + " ---")
            prevdepth = depth

        print(node)

        if type(node) is not Leaf and node.rest != None:
            queue.put(node.rest)
            atdepth[(currentdepth + 1) % 2] += 1
            print(str(bucket) + " - lhs: " +  str(node.rest))
        else:
            print(str(bucket))

        if bucket != None:
            for i in range(0, len(bucket)):
                    if type(bucket[bucket.iloc[i]]) != int:
                        queue.put(node.bucket[node.bucket.iloc[i]])
                        atdepth[(currentdepth + 1) % 2] += 1


        if atdepth[currentdepth] == 0:
            depth += 1
            currentdepth = (currentdepth + 1) % 2