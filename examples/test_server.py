import curio  
from curiorpc.application import Application
from curiorpc.server import TCPServer
from curiorpc.schemas import JsonRPCSchema 

import marshmallow as ma 
import os 
import logging 

import logging 
logging.basicConfig(level=logging.INFO)

app = Application()

class AddParamsSchema(ma.Schema):
    a = ma.fields.Integer()
    b = ma.fields.Integer()


class AddResultSchema(ma.Schema):
    c = ma.fields.Integer()


class AddSchema(JsonRPCSchema):
    params = ma.fields.Nested(AddParamsSchema)
    result = ma.fields.Nested(AddResultSchema)


if __name__ == '__main__':
    server = TCPServer(app)
    curio.run(server.listen, with_monitor=True) 