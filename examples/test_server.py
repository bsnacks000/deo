import asyncio  
from aiorpc.application import Application, ApplicationServer
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



def do_add(a, b):
    return {'c': a + b}


@app.entrypoint('AddSchema')
async def add(a, b):
    app = Application.current_app()
    return await app.loop.run_in_executor(app.processpool_executor, do_add, a, b)


if __name__ == '__main__':
    server = ApplicationServer(app)
    server.listen()