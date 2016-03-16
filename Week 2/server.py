from tornado.ioloop import IOLoop
from tornado import template
import tornado.web

import json, os

from yamr import Database, Chunk, Tree


def getDb():
    """
    Get the database object
    :return: Database object
    """
    return Database('test.db', max_size=4)

def CloseDb(db):
    """
    Close the given DB object
    :param db:
    """
    db.close()

def DbGetItems():
    """
    Get a list of all the items in the database
    :return: List with all DB items
    """
    db = getDb()

    items = db.items()

    itemArray = []

    for k, v in items:
        itemArray.append([str(k), str(v.decode('utf-8'))])
        #self.write(str(k) + ": " + str(v.decode('utf-8')) + "\n")

    CloseDb(db)

    return itemArray

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
    if os.path.isfile('test.db'):
        os.remove('test.db')

    db = getDb()

    # Add all the items to the new database
    for k, v in data_json.items():
        db[int(k)] = v

    db.commit()

    CloseDb(db)

class StoreHandler(tornado.web.RequestHandler):
    """
    This class handles all the requests to the API
    """

    def get(self):

        items = DbGetItems()
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

    def get(self):
        pass


class ApiInterface(tornado.web.RequestHandler):
    """
    Class for handling actions in the web interface
    """

    def get(self):

        db = Database('test.db', max_size=4)

        loader = template.Loader("templates")
        self.write(loader.load("index.html").generate(db=db, alert=None))

        db.close()

    def post(self):
        self.clear()
        self.set_status(405)
        self.finish("<html><body>POST not supported!</body></html>")

class CompactionHandler(tornado.web.RequestHandler):

    def get(self):
        """
        This class handles requests to compact the entire database
        """
        db = getDb()

        db.compaction()

        CloseDb(db)

        self.set_status(200)
        self.finish('Compaction of database was succesfull.')




class Application(tornado.web.Application):

    def __init__(self):
        handlers = [
            (r"/?", ApiInterface),
            (r"/api/v1/document/?", StoreHandler),
            (r'/static/(.*)', tornado.web.StaticFileHandler,
             {'path': 'static'}),
            (r"/api/v1/document/[0-9]+", SingleStoreHandler),
            (r"/api/v1/document/compaction", CompactionHandler)
        ]
        tornado.web.Application.__init__(self, handlers)


def main():

    app = Application()
    app.listen(8080)
    IOLoop.instance().start()

if __name__ == '__main__':
    main()
