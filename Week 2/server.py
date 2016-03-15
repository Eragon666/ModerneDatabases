from tornado import httpserver
from tornado import gen
from tornado.ioloop import IOLoop
from tornado import template
import sqlite3 as sqlite
import tornado.web

import sys

from yamr import Database, Chunk, Tree


class MainHandler(tornado.web.RequestHandler):

    def get(self):
        self.write('Hello, world!')


class StoreHandler(tornado.web.RequestHandler):

    def get(self):
        db = Database('test.db', max_size=4)
        for k, v in db.items():
            self.write(str(k) + ': ' + str(v.decode('utf-8')) + '\n')

        db.close()

    def post(self):
        self.write('POST - Welcome to the post document store!')

    def put(self):
        self.write('PUT - Welcome to our document store!')

    def delete(self):
        self.clear()
        self.set_status(501)
        self.finish("<html><body>Method not Implemented</body></html>")


class ApiInterface(tornado.web.RequestHandler):

    def get(self):

        db = Database('test.db', max_size=4)

        loader = template.Loader("templates")
        self.write(loader.load("index.html").generate(
            db=db, alert="Hallo Xander", alert_type="success", current_user="anonymous"))

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
