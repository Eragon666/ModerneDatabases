from tornado import httpserver
from tornado import gen
from tornado.ioloop import IOLoop
from tornado import template
import sqlite3 as sqlite
import tornado.web

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write('Hello, world!')

class StoreHandler(tornado.web.RequestHandler):
    def get(self):
        self.write('GET - Welcome to our document store!')

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

        loader = template.Loader("templates")
        self.write(loader.load("index.html").generate(myvalue="Wow this is awesome VIA", alert=None, current_user="anonymous"))

        #t = template.Template("<html>{{ myvalue }}</html>")
        #self.write(t.generate(myvalue="XXX"))

    def post(self):
        self.clear()
        self.set_status(405)
        self.finish("<html><body>POST not supported!</body></html>")

def verifyDatabase():
    conn = sqlite.connect('cars.db')
    c = conn.cursor()
    try:
        c.execute('SELECT * FROM cars')
        print('Table already exists')
    except:
        print('Creating table \'cars\'')
        c.execute('CREATE TABLE cars (\
            id text,\
            make text,\
            model text,\
            year text,\
            trans text,\
            color text)')
        print('Successfully created table \'cars\'')
    conn.commit()
    conn.close()

class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/?", ApiInterface),
            (r"/api/v1/document/?", StoreHandler),
            (r'/static/(.*)', tornado.web.StaticFileHandler, {'path': 'static'}),
            (r"/api/v1/document/[0-9][0-9][0-9][0-9]/?", StoreHandler)
        ]
        tornado.web.Application.__init__(self, handlers)

def main():

    verifyDatabase()

    app = Application()
    app.listen(8080)
    IOLoop.instance().start()

if __name__ == '__main__':
    main()