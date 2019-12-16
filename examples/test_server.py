import asyncio  
from aiorpc.application import Application
from aiorpc.server import TCPServer
from aiorpc.schemas import JsonRPCSchema 

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
    asyncio.run(server.listen())