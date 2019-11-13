""" 
"""
import asyncio
from .parsers import JSONByteParser
from .registry import EntrypointRegistry 
from .exceptions import AiorpcException, ParseError, MethodNotFound, \
    InvalidRequest, InvalidParams, InternalError, ChainError
from .schemas import ContextData

import orjson

import concurrent.futures 
import logging 
import functools
import multiprocessing
import marshmallow as ma 

import traceback 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# these extra handlers are need in the threadpool


def rpc_error_handler(method):

    @functools.wraps(method)
    def _inner(*args, **kwargs):
        try:
            return method(*args, **kwargs)
        except AiorpcException as err: 
            #handler here     

    return _inner 



class ContextDataHandler(object):
    """ used internally by methodology to handle the contextdata object. Proxies many calls to
    the contexted data interface and has access to the contextdata's related schema.
    """

    def __init__(self, data, schema_class, **schema_kwargs):
        self.data = data 
        self._schema = schema_class(**schema_kwargs)
        self._contextdata = self._schema.load(self.data)

    @property
    def contextdata(self):
        return self._contextdata


    def get_id(self):
        return self._contextdata.id 


    def get_params(self):
        return self._contextdata.params 


    def write_to_result(self, result):
        """ Assign result object onto contextdata
        """
        self._contextdata.result = result 

    def write_to_error(self, err):
        """ Assign an error object to the context
        """
        self._contextdata.error = err 


    def dump_data(self):
        return self._schema.dump(self._contextdata) 


class Application(object):
    """ The application object handles the entrypoint registry and manages the event loop. 
    An instance is passed to the TCPServer protocol. 

    TODO methods for handling dask client. 
    """

    def __init__(self, threadpool_max_workers=None):
        self._entrypoint_registry = EntrypointRegistry()
        if threadpool_max_workers is None:
            threadpool_max_workers = multiprocessing.cpu_count() * 2 + 1 
        self.threadpool_max_workers = threadpool_max_workers
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=threadpool_max_workers)
        
        self.loop = asyncio.get_event_loop()
        self.parser = JSONByteParser()


    @property 
    def entrypoint(self):
        return self._entrypoint_registry




    
    def _handle_error_detail_with_contextdata_handler(self, exc, contextdata_handler, detail={}):
        """ This handler is used inside the threadpool. This assumes that we have a valid 
        contextdata that can write directly to the response object.
        """
        detail = {'code': exc.error_code, 'message': exc.message, 'data':{'detail': detail}}
        contextdata_handler.write_to_error(detail)      
        return contextdata_handler.dump_data()


    def _handle_full_error(self, exc, data):
        """ This handles errors by writing a response directly when we don't have a contextdata object.
        """
        id_ = data['id'] if 'id' in data else None 
        exc.obj = self.parser.encode({"jsonrpc": "2.0", "error": {"code": exc.error_code, "message": exc.message}, "id": id_})
        return exc 

    def _prepare_contextdata(self, data):

        contextdata_handler = ContextDataHandler(data)  # <--- push data into contextdata handler 
   
        if 'method' not in data:
            logger.error('Invalid request')
            exc = InvalidRequest() # < no context yet this is a problem for error handling.
            exc = self._handle_full_error(exc, data)
            raise exc 

        try:
            entry = self._entrypoint_registry.get_entrypoint(data['method']) 
            contextdata_handler.initialize_schema(entry.schema_class)   

        except KeyError as err:
            logger.error('Method not found')
            exc = MethodNotFound()
            exc = self._handle_full_error(exc, data)
            raise exc  

        except ma.ValidationError as err:
            logger.error('Invalid request')
            exc = InvalidRequest()
            exc = self._handle_full_error(exc, data) 
            raise exc 
        
        return entry, contextdata_handler

    # TODO << this is not finished... error handling needed. All errors should map to an error object call with RPC errors.
    @rpc_error_handler
    def _run_in_executor(self, data):
        """ This is the main call that gets run on the threadpool executor. Most RPC exceptions are handled here except a 
        few that can occur before we have access to a context handler. 
        """

        entry, contextdata_handler = self._prepare_contextdata(data)
        params = contextdata_handler.get_params()

        if params is None:
            res = entry.func()
        elif isinstance(params, (list, tuple)):
            res = entry.func(*params)
        else:
            res = entry.func(**kwargs)

        contextdata_handler.write_to_result(res)
        
        if contextdata_handler.get_id() is not None:
            return contextdata_handler.dump_data()


    async def handle_batch_request(self, requests):
        futures = [self.loop.run_in_executor(self._executor, self._run_in_executor, req) for req in requests]
        return await asyncio.gather(*futures)


    async def handle_single_request(self, request):
        return await self.loop.run_in_executor(self._executor, self._run_in_executor, request) 



class TCPServer(object):
    """ Simple TCPServer that runs the application.
    """

    class _JSONRPCProtocol(asyncio.Protocol):

        def __init__(self, app): 
            self.app = app 
            self.loop = asyncio.get_event_loop()
            self.parser = JSONByteParser()


        def connection_made(self, transport):
            peername = transport.get_extra_info('peername')
            logger.info(' <----- ( •_•) Connection from {}'.format(peername))
            self.transport = transport

        
        def data_received(self, data):
            logger.info(' <---- ʕ•ᴥ•ʔ Data received: {!r}'.format(data))
            try:
                data = self._decode_request(data)
            except ParseError as err:
                self.transport.write(orjson.dumps({"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}, "id": None})) # <-- short circut and send a standard error. No need to even put
                self.transport.close()
                return 

            if isinstance(data, list):
                task = self.loop.create_task(self.app.handle_batch_request(data))
            else:
                task = self.loop.create_task(self.app.handle_single_request(data))
            task.add_done_callback(self._task_response_callback)    


        def _task_response_callback(self, task):
            data = task.result()  # <-- returns from app.handle_request     
            if data is not None:
                logger.info(' ----> (~‾▿‾)~  Sending: {!r}'.format(data))
                
                try:
                    data = self._encode_response(data)
                except ParseError as err:
                    data = orjson.dumps({"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}, "id": None})
                
                self.transport.write(data)
            logger.info(' ----> Closing Connection:  ¯\_(ツ)_/¯')
            self.transport.close()

        def _decode_request(self, data):
            try:
                return self.parser.decode(data)
            except orjson.JSONDecodeError as err:
                raise ParseError from err 

        def _encode_response(self, obj):
            try:
                if obj['id'] is not None:  # <-- handles rpc-notifications though we don't plan on using it.
                    return self.parser.encode(obj)
            except orjson.JSONEncodeError as err:
                raise ParseError from err 

    def __init__(self, app):
        self.app = app 


    def listen(self, addr='127.0.0.1', port=6666):

        loop = asyncio.get_event_loop()
        coro = loop.create_server(lambda: self._JSONRPCProtocol(self.app), addr, port)
        server = loop.run_until_complete(coro)

        # Serve requests until Ctrl+C is pressed
        logger.info(' ----> ฅ^•ﻌ•^ฅ Serving on {} with {} threads...'.format(server.sockets[0].getsockname(), self.app.threadpool_max_workers))
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        # Close the server
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
