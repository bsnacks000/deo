import asyncio  
from aiorpc.application import TCPServer, Application 
from aiorpc.schemas import JsonRPCSchema 

import marshmallow as ma 
from distributed import Client, as_completed
import argparse  
import os 

app = Application()


class AddParamsSchema(ma.Schema):
    a = ma.fields.Integer()
    b = ma.fields.Integer()


class AddResultSchema(ma.Schema):
    c = ma.fields.Integer()


class AddSchema(JsonRPCSchema):
    params = ma.fields.Nested(AddParamsSchema)
    result = ma.fields.Nested(AddResultSchema)



@app.entrypoint.register('AddSchema')
def add(a, b):
    return {'c': a + b} 


def _add_on_dask(a, b):
    print('---> got task on {}'.format(os.getpid()))
    return a + b


@app.entrypoint.register('AddSchema')
def add_on_dask(a, b):
    fs = [app.dask_client.submit(_add_on_dask, a + i, b+ i) for i in range(1000)] 
    res = []
    for f in as_completed(fs):
        res.append(f.result())

    return {'c': sum(res) }



if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--scheduler-address', help='address for remote dask-scheduler')
    
    args = parser.parse_args()

    if args.scheduler_address:
        print(args)
        app.set_dask_client(Client(args.scheduler_address))
        print(app.dask_client)
    else:
        app.set_dask_client(Client(processes=True)) 
    
    server = TCPServer(app)
    server.listen()