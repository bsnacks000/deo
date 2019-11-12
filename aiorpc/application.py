""" 
"""
import asyncio
from .parsers import JSONByteParser
from .registry import EntrypointRegistry 
from .exceptions import AiorpcException, ParseError, MethodNotFound, \
    InvalidRequest, InvalidParams, InternalError, ChainError

from orjson import JSONDecodeError, JSONEncodeError

import concurrent.futures 
import logging 
import functools
import multiprocessing
import marshmallow as ma 

import traceback 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



def _handle_error_detail_from_exception(exc, contextdata_handler, data={}):
    """ This handler is used inside the threadpool. 
    """
    detail =  {
        'code': exc.error_code, 
        'message': exc.message, 
        'data': data
    }
    contextdata_handler.write_to_error(detail)      
    return contextdata_handler.dump_data()


def _handle_bad_decode():
    """ Since we might have to parse a list this gets before a context is established. 
    """
    return {"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error"}, "id": null}


def _handle_null_batch(data):
    """ This is a special handler because we don't have a context yet. 
    """
    return {"jsonrpc": "2.0", "error": {"code": -32600, "message": "Invalid Request"}, "id": null}


def _check_params(params, entry):
    # XXX not sure the best way to check. In reality we need type info. 
    pass 

class ContextDataHandler(object):
    """ used internally by methodology to handle the contextdata object. Proxies many calls to
    the contexted data interface and has access to the contextdata's related schema.
    """

    def __init__(self, data):
        self.data = data 
        self._schema = None
        self._contextdata = None

    @property
    def contextdata(self):
        return self._contextdata


    def initialize_schema(self, schema_class, **schema_kwargs):
        """ Initialize the correct schema and load the object.
        """
        self._schema = schema_class(**schema_kwargs)
        self._contextdata = self._schema.load(self.data)


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


    def _decode_request(self, data):
        try:
            return self.parser.decode(data)
            return d 
        except JSONDecodeError as err:
            raise ParseError from err 

    def _encode_response(self, obj):
        try:
            if obj['id'] is not None:
                return self.parser.encode(obj)
        except JSONEncodeError as err:
            raise ParseError from err 

    # TODO << this is not finished... error handling needed. All errors should map to an error object call with RPC errors.
    def _run_in_executor(self, data):
        """ Blocking calls happen in a threadpool executor
        """        
        try:
            contextdata_handler = ContextDataHandler(data)  # <--- push data into contextdata handler 
   
            if 'method' not in data:
                logger.error('Invalid request')
                raise InvalidRequest # < no context yet 

            try:
                entry = self._entrypoint_registry.get_entrypoint(data['method'])  # <-- XXX error handling 
            except KeyError as err:
                logger.error('Method not found')
                raise MethodNotFound from err  # < not context yet 

            try:
                contextdata_handler.initialize_schema(entry.schema_class)   # <--- XXX error on load must be handled
            except ma.ValidationError as err:
                logger.error('Invalid request')
                raise InvalidRequest from err  
                    
            params = contextdata_handler.get_params() 
            _check_params(params)

            try:
                if params is None:
                    res = entry.func()
                elif isinstance(params, list): 
                    res = entry.func(*params)
                else:
                    res = entry.func(**params)
                contextdata_handler.write_to_result(res)    
            except ChainError as err: 
                logger.error('A Chain error occurred')
                data = _handle_error_detail_from_exception(err, contextdata_handler, data=err.context) 
                
            except Exception as err:
                logger.error('An unhandled exception occured.')
                raise InternalError from err 

        except AiorpcException as err:
            data = _handle_error_detail_from_exception(err, contextdata_handler, data={'detail': traceback.format_tb()}) 
    
        finally:
            return self._encode_response(data)


    async def handle_request(self, raw_request):
        """ Parse the method from the raw request object, create the context data and run. If its a list we run 
        all the rquests and await the responses.
        """
        # We need to decode the data first so we run that seperately and only pass into the main method if it is valid.
        # This is because of how RPC spec handles batch (arrays) vs. single object requests.
        # Almost everything else is handled inside the thread (since we are ultimately running blocking functions)
        try:
            data = await self.loop.run_in_executor(self._executor, self._decode_request, raw_request)
        except ParseError as err:
            return _handle_bad_decode()

        if isinstance(data, list):
            if len(data) == 0:  # handle empty list here (technically valid json)
                return _handle_null_batch(data)
            futures = [self.loop.run_in_executor(self._executor, self._run_in_executor, req) for req in data]        
            return await asyncio.gather(*futures)
        else:
            return await self.loop.run_in_executor(self._executor, self._run_in_executor, data)


class _JSONRPCProtocol(asyncio.Protocol):

    def __init__(self, app): 
        self.app = app 
        self.loop = asyncio.get_event_loop()

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        logger.info(' <----- ( •_•) Connection from {}'.format(peername))
        self.transport = transport

    
    def data_received(self, data):
        
        logger.info(' <---- ʕ•ᴥ•ʔ Data received: {!r}'.format(data))
        task = self.loop.create_task(self.app.handle_request(data))
        task.add_done_callback(self._task_response_callback)    


    def _task_response_callback(self, task):
        # error handler needs to be wrapped around this one 
        data = task.result()  # <-- returns from app.handle_request     
        if data is not None:
            logger.info(' ----> (~‾▿‾)~  Sending: {!r}'.format(data))
            self.transport.write(data)

        logger.info(' ----> Closing Connection:  ¯\_(ツ)_/¯')
        self.transport.close()



class TCPServer(object):
    """ Simple TCPServer that runs the application.
    """

    def __init__(self, app):
        self.app = app 


    def listen(self, addr='127.0.0.1', port=6666):

        loop = asyncio.get_event_loop()
        coro = loop.create_server(lambda: _JSONRPCProtocol(self.app), addr, port)
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
