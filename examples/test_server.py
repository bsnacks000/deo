import asyncio  
from aiorpc.application import TCPServer, Application, create_server
from aiorpc.schemas import JsonRPCSchema 

import marshmallow as ma 
from distributed import Client, as_completed, LocalCluster
import argparse  
import os 
import logging 

from aiorpc.chain import ChainableMappingProxy
from tests.test_chain import FakeChain 

app = Application()


class AddParamsSchema(ma.Schema):
    a = ma.fields.Integer()
    b = ma.fields.Integer()


class AddResultSchema(ma.Schema):
    c = ma.fields.Integer()


class AddSchema(JsonRPCSchema):
    params = ma.fields.Nested(AddParamsSchema)
    result = ma.fields.Nested(AddResultSchema)


class AddDaskResultSchema(ma.Schema):
    a = ma.fields.Integer()
    b = ma.fields.Integer()
    c = ma.fields.Integer()
    d = ma.fields.Integer()


class AddDaskSchema(JsonRPCSchema):
    params = ma.fields.Nested(AddParamsSchema) 
    result = ma.fields.Nested(AddDaskResultSchema)


@app.entrypoint.register('AddSchema')
def add(a, b):
    return {'c': a + b} 


def _add_on_dask(a, b):
    print('---> got task on {}'.format(os.getpid()))
    return a + b


@app.entrypoint.register('AddSchema')
def add_on_dask(a, b):
    fs = [app.dask_client.submit(_add_on_dask, a + i, b+i) for i in range(100)] 
    res = []
    for f in as_completed(fs):
        res.append(f.result())

    return {'c': sum(res) }


@app.entrypoint.register('AddDaskSchema')
def add_dask_chain(a, b):
    proxy = ChainableMappingProxy({'a': a, 'b': b})
    chn = FakeChain().dask_make_c_from_ab(some_val=42).dask_make_d_from_c(some_val=42)
    return chn(proxy)
    

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--scheduler-address', help='address for dask-scheduler')
    
    args = parser.parse_args()
    server = create_server(app, args.scheduler_address, log_level='INFO', processes=True)
    server.listen()