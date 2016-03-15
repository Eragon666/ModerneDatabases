from tornado.ioloop import IOLoop
from tornado import template
import tornado.web

import json, os

from yamr import Database, Chunk, Tree


class MainHandler(tornado.web.RequestHandler):

    def get(self):
        self.write('Hello, world!')


class StoreHandler(tornado.web.RequestHandler):

    def get(self):
        db = Database('test.db', max_size=4)

        self.set_status(200)

        i = 0

        for k, v in db.items():
            i += 1
            self.write(str(k) + ": " + str(v.decode('utf-8')) + "\n")

        db.close()

        self.finish("Succesfully retrieved all " + str(i) + " items from the database.\n")

    def post(self):
        data = self.request.body
        value = data.decode("utf-8")

        db = Database('test.db', max_size=4)

        # Get the last key in the database.
        key = max(db.keys(), key=int) + 1

        db[int(key)] = value

        db.commit()

        db.close()

        self.set_status(200)
        self.finish("Document inserted in key " + str(key) + "\n")

    def put(self):
        # The old file can be deleted. Put will replace all the content.

        if os.path.isfile('test.db'):
            os.remove('test.db')

        # Load the data from the request and decode to utf-8
        data = self.request.body
        data_json = json.loads(data.decode("utf-8"))

        db = Database('test.db', max_size=4)

        # Add all the items to the new database
        for k, v in data_json.items():
            db[int(k)] = v

        db.commit()

        db.close()

        self.set_status(200)
        self.finish("PUT action successfull\n")


    def delete(self):
        self.clear()
        self.set_status(501)
        self.finish("Method not Implemented\n")


class ApiInterface(tornado.web.RequestHandler):

    def get(self):

        db = Database('test.db', max_size=4)

        loader = template.Loader("templates")
        self.write(loader.load("index.html").generate(db=db, alert=None, current_user="anonymous"))

        db.close()

    def post(self):
        self.clear()
        self.set_status(405)
        self.finish("<html><body>POST not supported!</body></html>")


class Application(tornado.web.Application):

    def __init__(self):
        handlers = [
            (r"/?", ApiInterface),
            (r"/api/v1/document/?", StoreHandler),
            (r'/static/(.*)', tornado.web.StaticFileHandler,
             {'path': 'static'}),
            (r"/api/v1/document/[0-9][0-9][0-9][0-9]/?", StoreHandler)
        ]
        tornado.web.Application.__init__(self, handlers)


def main():

    app = Application()
    app.listen(8080)
    IOLoop.instance().start()

if __name__ == '__main__':
    main()
