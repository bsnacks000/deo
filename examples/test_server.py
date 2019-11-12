import asyncio  
from aiorpc.application import TCPServer, Application 
from aiorpc.schemas import JsonRPCSchema 

import marshmallow as ma 


class AddParamsSchema(ma.Schema):
    a = ma.fields.Integer()
    b = ma.fields.Integer()


class AddResultSchema(ma.Schema):
    c = ma.fields.Integer()


class AddSchema(JsonRPCSchema):
    params = ma.fields.Nested(AddParamsSchema)
    result = ma.fields.Nested(AddResultSchema)


app = Application()


@app.entrypoint.register('AddSchema')
def add(a, b):
    return {'c': a + b} 

    
server = TCPServer(app)
server.listen()