from tornado.ioloop import IOLoop
from tornado import template
import tornado.web
from asteval_wrapper import Script
from yamr import Database, Chunk, Tree
import json, os


def getDb(document='test.db'):
    """
    Get the database object
    :return: Database object
    """
    return Database(document, max_size=4)

def CloseDb(db):
    """
    Close the given DB object
    :param db:
    """
    db.close()

def DbGetItems(key = None):
    """
    Get a list of all the items in the database. If key is given, only the
    item with the given key is returned
    :return: List with all DB items
    """
    db = getDb()

    print(key)

    if key is None:

        items = db.items()

        result = []

        for k, v in items:
            result.append([str(k), str(v.decode('utf-8'))])

    else:
        result = db[int(key)].decode('utf-8')

    CloseDb(db)

    return result

def DbPostItems(value):
    """
    Add a value to the database
    :param value:
    :return: key of new item
    """
    db = getDb()

    # Get the last key in the database.
    key = max(db.keys(), key=int) + 1

    db[int(key)] = value

    db.commit()

    CloseDb(db)

    return key

def DbPutItems(data_json):

    # The old file can be deleted. Put will replace all the content.
    removeFile('test.db')

    db = getDb()

    # Add all the items to the new database
    for k, v in data_json.items():
        db[int(k)] = v

    db.commit()

    CloseDb(db)

def removeFile(file):
    """
    Remove the specified file if available
    :param file:
    :return:
    """
    if os.path.isfile(file):
        os.remove(file)

class StoreHandler(tornado.web.RequestHandler):
    """
    This class handles all the requests to the API
    """

    def get(self):

        items = DbGetItems(None)
        self.set_status(200)
        i = 0

        for k, v in items:
            i += 1
            self.write(str(k) + ": " + str(v) + "\n")

        self.finish("Succesfully retrieved all " + str(i) + " items from the database.\n")

    def post(self):

        data = self.request.body
        value = data.decode("utf-8")

        key = DbPostItems(value)

        self.set_status(200)
        self.finish("Document inserted in key " + str(key) + "\n")

    def put(self):

        # Load the data from the request and decode to utf-8
        data = self.request.body
        data_json = json.loads(data.decode("utf-8"))

        DbPutItems(data_json)

        self.set_status(200)
        self.finish("PUT action successfull\n")


    def delete(self):
        self.clear()
        self.set_status(501)
        self.finish("Method not Implemented\n")


class SingleStoreHandler(tornado.web.RequestHandler):

    def get(self, key):
        """
        Retrieve a document with the given key
        :param key:
        :return:
        """

        result = DbGetItems(key)

        self.set_status(200)
        self.finish("Requested key " + key + " gives back value = " + result + "\n")

    def put(self, key):
        data = self.request.body
        data_json = data.decode("utf-8")

        db = getDb()

        if int(key) in db:
            db[int(key)] = data_json
            self.set_status(200)
            self.finish("Document with key = " + key + " updated to the received value.\n")
        else:
            self.set_status(409)
            self.finish("Key " + key + " does not exist!\n")

        db.commit()

        CloseDb(db)


class ApiInterface(tornado.web.RequestHandler):
    """
    Class for handling actions in the web interface
    """

    def get(self):

        db = getDb()

        loader = template.Loader("templates")
        self.write(loader.load("index.html").generate(db=db, alert=None))

        CloseDb(db)

    def post(self):
        self.clear()
        self.set_status(405)
        self.finish("<html><body>POST not supported!</body></html>")

class MapReduceInterface(tornado.web.RequestHandler):
    """
    Class for handling actions in the web interface
    """

    def get(self):

        db = getDb()

        loader = template.Loader("templates")
        self.write(loader.load("mapreduce.html").generate(db=db, alert=None))

        CloseDb(db)

    def post(self):
        self.clear()
        self.set_status(405)
        self.finish("<html><body>POST not supported!</body></html>")



class CompactionHandler(tornado.web.RequestHandler):
    """
    This class handles requests to compact the entire database
    """

    def get(self):
        db = getDb()

        db.compaction()

        CloseDb(db)

        self.set_status(200)
        self.finish('Compaction of database was succesfull.')


class MapHandler(tornado.web.RequestHandler):
    """
    Map handler
    """

    def post(self):

        mapscript = self.get_argument('mapscript', None)
        reducescript = self.get_argument('reducescript', None)

        emitfile = self.get_argument('emit', 'emit.py')
        reducefile = self.get_argument('reduce', 'reduce.py')

        mr = MapReduce()
        mr.map(self, emitfile, reducefile, mapscript, reducescript)
        mr.mapResult(self)

    def get(self):
        """
        Execute the map function
        :return:
        """
        emitfile = 'emit.py'
        reducefile = 'reduce.py'

        mr = MapReduce()
        mr.map(self, emitfile, reducefile)
        mr.mapResult(self)

class MapResultHandler(tornado.web.RequestHandler):
    """
    Return the result of the map function
    """

    def get(self):
        mr = MapReduce()
        mr.mapResult(self)


class ReduceHandler(tornado.web.RequestHandler):
    """
    Run the reduce operation. By default uses reduce.py.
    Returns the result of the reduce operation
    """

    def post(self):

        reducefile = self.get_argument('reduce', 'reduce.py')
        script = self.get_argument('script', None)

        mr = MapReduce()
        mr.reduce(self, reducefile, script)
        mr.reduceResult(self)

    def get(self):
        reducefile = 'reduce.py'

        mr = MapReduce()

        mr.reduce(self, reducefile)
        mr.reduceResult(self)

class ReduceResultHandler(tornado.web.RequestHandler):
    """
    Show the results from the reduce operation
    """

    def get(self):
        mr = MapReduce()
        mr.reduceResult(self)

class MapReduce():
    """
    This class contains all functions for Map Reduce
    """

    def map(self, web, emitfile, reducefile, emitscript, reducescript):

        removeFile('emit.db')

        # Open a new asteval wrapper and add the necessary files
        mrScript = Script()

        # Load from files if emitscript is None
        if emitscript is None:
            mrScript.add_file(emitfile)
            mrScript.add_file(reducefile)
            if len(mrScript.interpreter.error) > 0:
                web.write('Incorrect MapReduce file(s)\n')
                web.write(str(mrScript.interpreter.error[0].get_error()))

        else:
            mrScript.add_scriptstring(emitscript)
            mrScript.add_scriptstring(reducescript)
            if len(mrScript.interpreter.error) > 0:
                web.write('Incorrect MapReduce file(s)\n')
                web.write(str(mrScript.interpreter.error[0].get_error()))

        mrScript.symtable['emit_dict'] = {}

        db = getDb()
        tmp_db = getDb('emit.db')

        # Forward all the documents to the map query
        for v in db.values():

            mrScript.symtable['emit_dict'] = {}

            mrScript.invoke('dbMap', doc=v)

            # Store the value and key in the emit_dict
            emit_dict = mrScript.symtable['emit_dict']

            for tmp_k, tmp_v in emit_dict.items():
                if tmp_k in tmp_db:
                    tmp_db[tmp_k].extend(tmp_v)
                else:
                    tmp_db[tmp_k] = tmp_v

        tmp_db.commit()
        CloseDb(db)
        CloseDb(tmp_db)

        web.write("Map function executed!<br>\n")

    def mapResult(self, web):
        # Retrieve the tmp database used for storing the results of the mapper
        tmp_db = getDb('emit.db')

        web.set_status(200)

        for k,v in tmp_db.items():
            web.write(str(k) + ': ' + str(v) + '<br>\n')

        web.finish("All items retrieved\n")

        CloseDb(tmp_db)

    def reduce(self, web, reducefile, script):
        # Remove the file from the previous reduce
        removeFile('reduce.db')

        mrScript = Script()

        if script is None:
            mrScript.add_file(reducefile)
        else:
            mrScript.add_scriptstring(script)

        # Get the emit database and make a new reduce database
        tmp_db = getDb('emit.db')
        reduce_db = getDb('reduce.db')

        # Now every key value pair has to be processed
        for k, v in tmp_db.items():
            value = mrScript.invoke('dbReduce', key=k, values=v)
            reduce_db[k] = str(value)

        reduce_db.commit()

        CloseDb(tmp_db)
        CloseDb(reduce_db)

        web.write("Reduce function executed!\n")

    def reduceResult(self, web):
        reduce_db = getDb('reduce.db')

        g = ((k, reduce_db[k]) for k in sorted(reduce_db, key=reduce_db.get, reverse=True))

        for k, v in g:
            web.write(k.decode("utf-8") + " : " + str(v.decode("utf-8")) + "\n")

        CloseDb(reduce_db)

        web.set_status(200)
        web.finish("Done!\n")


class Application(tornado.web.Application):

    def __init__(self):
        handlers = [
            (r"/?", ApiInterface),
            (r"/mapreduce/?", MapReduceInterface),
            (r"/api/v1/documents", StoreHandler),
            (r"/api/v1/document/([0-9]+)", SingleStoreHandler),
            (r'/static/(.*)', tornado.web.StaticFileHandler,
             {'path': 'static'}),
            (r"/api/v1/documents/compact", CompactionHandler),
            (r"/api/v1/map", MapHandler),
            (r"/api/v1/map/result", MapResultHandler),
            (r"/api/v1/reduce", ReduceHandler),
            (r"/api/v1/reduce/result", ReduceResultHandler)
        ]
        tornado.web.Application.__init__(self, handlers)


def main():

    app = Application()
    app.listen(8080)
    IOLoop.instance().start()

if __name__ == '__main__':
    main()
