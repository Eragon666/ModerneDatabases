#!env/bin/python

from flask import *
from yamr import Database, Chunk, Tree, Script
from sortedcontainers import SortedDict
from flask.ext.restful import Resource, Api
import os
import json


app = Flask(__name__)
app.config['DEBUG'] = True
api = Api(app)

name = 'test.db'


def emit(key, value):
    print('emit: ', key, value)
    mr_db = Database("mapred.db", max_size=100)

    try:
        emitlist = mr_db[key]
    except:
        emitlist = None

    if(emitlist == None):
        print('probles?')
        # mr_db[key] = [value]
    # else:
    #     print('add to db')
    #     mr_db[key].append(value)

    mr_db.commit()
    mr_db.close()


class Collection(Resource):

    def get(self):
        db = Database(name, max_size=50)
        docs = dict()

        for key in db:
            docs[key] = db.tree[key].decode('utf-8')

        db.close()
        return str(docs)

    def post(self):
        # ficx hier de json decode!
        data = request.get_json(force=True)
        key, document = json.decode(data)
        print(key, document)

        db = Database(name, max_size=50)

        db[key] = document
        db.commit()
        db.compact()
        db.close()
        return "Inserted: "+str({i: document})

    def put(self):
        open(name, 'w').close()
        data = request.get_json(force=True)
        key, document = json.decode(data)

        # create array of document
        keys = [x.strip() for x in keys.split(',')]
        documents = [x.strip() for x in documents.split(',')]

        db = Database(name, max_size=50)

        # check of dit werkt
        for key, document in keys, documents:
            db[key] = document

        db.commit()
        db.compact()
        db.close()
        return 'Database successfully replaced'

    def delete(self):
        open(name, 'w').close()
        return 'Database emptied succesfully.'


class Entity(Resource):
    # REST on single doc

    def get(self, doc_id):
        db = Database(name, max_size=50)

        try:
            value = str(db[doc_id].decode('utf-8'))
        except:
            return "Document Not Found"

        db.close()
        return value

    def put(self, doc_id):
        value = request.form['data']
        db = Database(name, max_size=50)

        db[doc_id] = value
        db.close()
        return "Document "+str(doc_id)+" value set to "+value

    def delete(self, doc_id):
        return "Not currently supported..."
        try:
            db = Database(name, max_size=50)
        except:
            return "Collection not found... "

        try:
            db.__delitem__(doc_id)
        except:
            return "Document not found"

        db.close()
        return "Document "+str(doc_id)+" deleted succesfully."


class MapReduce(Resource):

    def post(self):
        open('mapred.db', 'w').close()
        # get functions from curl request
        mapreduce = request.get_json(force=True)

        # split funcs
        mapfunc = mapreduce['map']
        reducefunc = mapreduce['reduce']

        # open entire database
        db = Database(name, max_size=50)

        # create new asteval scripter
        script = Script()
        script.symtable["emit"] = emit
        script.add_string(mapfunc)
        script.add_string(reducefunc)

        mr_db = Database("mapred.db", max_size=50)
        mr_db[25] = 'testvalue'
        mr_db.close()

        # iterate over each key in original database: run map function
        for key in db:
            print('map:', key, db[key][0][0][0][0].decode('utf-8'))
            script.invoke(
                "map", key=key, value=db[key][0][0][0][0].decode('utf-8'))

        result = []

        # # iterate over keys in temp db: run reduce
        # for key in mr_db:
        #     print('reduce:', key, mr_db[key])
        #     result.append(script.invoke("reduce", key=key, value=db[key]))

        open('mapred.db', 'w').close()
        return str(result)


api.add_resource(Collection, '/documents')
api.add_resource(Entity, '/document/<int:doc_id>')
api.add_resource(MapReduce, '/mapred')


def main():
    app.run(port=8080)

if __name__ == "__main__":
    main()
