""" 
"""
import asyncio
from .parsers import JSONByteParser
from .registry import EntrypointRegistry 
from .exceptions import AiorpcException, ParseError, MethodNotFound, \
    InvalidRequest, InvalidParams, InternalError, ChainError, RegistryEntryError
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
    """ A decorator that handles the logic of reporting the various RPC errors.
    """
    @functools.wraps(method)
    def _inner(*args, **kwargs):
        try:
            return method(*args, **kwargs)

        except ChainError as err: 
            # if using the chain API we can guarantee that any data run in the chain can be preserved
            # we use the error detail for this...           
            detail = {
                'code': err.error_code, 
                'message': err.message, 
                'data': {'detail': err.context }
            }
            err.contextdata_handler.write_to_error(detail)
            return err.contextdata_handler.dump_data()
            
        except AiorpcException as err: 
            # We check the state of the contextdata_handler... If it was correctly initialized we can write the detail
            # if not then we try to extract the id from the raw data packet and return 
            if err.contextdata_handler._is_initialized:
                detail = {
                    'code': err.error_code, 
                    'message': err.message, 
                    'data':{'detail': traceback.format_tb(err.__traceback__)}}    
                err.contextdata_handler.write_to_error(detail)      
                return err.contextdata_handler.dump_data()
            else:
                id_ = err.data['id'] if 'id' in err.data else None 
            return {"jsonrpc": "2.0", "error": {"code": err.error_code, "message": err.message}, "id": id_}    

    return _inner 



class ContextDataHandler(object):
    """ used internally by Application to handle the contextdata object. Proxies many calls to
    the contexted data interface and has access to the contextdata's related schema.
    """

    def __init__(self, data):
        self._data = data
        self._schema = None 
        self._contextdata = None 
        self._is_initialized = False

    @property
    def contextdata(self):
        return self._contextdata

    @property 
    def data(self):
        return self._data 


    def load(self, entry):
        """ We breakup the initialization here. This is to help with error handling. 
        """
        self._schema = entry.schema_class(**schema_kwargs)
        self._contextdata = self._schema.load(self._data)
        self._is_initialized = True

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
        self._threadpool_max_workers = threadpool_max_workers
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=threadpool_max_workers)
        
        self.loop = asyncio.get_event_loop()
        self.parser = JSONByteParser()


    @property 
    def entrypoint(self):
        return self._entrypoint_registry


    def _handle_rpc_error(self, rpc_exc, original_exc, contextdata_handler):
        """ This assures that errors are logged and that a contextdata_handler is bound to the exception. 
        We raise the the rpc_exc from the original here. 
        """
        logger.error(rpc_exc.message)
        rpc_exc.cxt_handler = contextdata_handler
        raise rpc_exc from original_exc 


    def _prepare_contextdata(self, data):
        """ This takes the raw data. 
        """
        contextdata_handler = ContextDataHandler(data) # <--- push data into contextdata handler
        try:
            entry = self._entrypoint_registry.get_entrypoint(data['method']) 
            contextdata_handler.load(entry)

        except KeyError as err:
            exc = MethodNotFound()
            self._handle_rpc_error(exc, err, contextdata_handler)

        except ma.ValidationError as err:
            exc = InvalidRequest()
            self._handle_rpc_error(exc, err, contextdata_handler)

        return entry, contextdata_handler

    # TODO << this is not finished... error handling needed. All errors should map to an error object call with RPC errors.
    @rpc_error_handler
    def _run_in_executor(self, data):
        """ This is the main call that gets run on the threadpool executor. Most RPC exceptions are handled here except a 
        few that can occur before we have access to a context handler. 
        """

        entry, contextdata_handler = self._prepare_contextdata(data)
        params = contextdata_handler.get_params()

        try:
        
            if params is None:
                res = entry.func()
            elif isinstance(params, (list, tuple)):
                res = entry.func(*params)
            else:
                res = entry.func(**kwargs)

            contextdata_handler.write_to_result(res)
            
            if contextdata_handler.get_id() is not None:
                return contextdata_handler.dump_data()

        except Exception as err: 
            exc = InternalError()
            self._handle_rpc_error(exc, err, contextdata_handler)

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
            except ParseError as err:  # <--- we short circuit here... 
                self.transport.write(orjson.dumps(
                    {"jsonrpc": "2.0", "error": {"code": -32700, "message": "Parse error on Decode"}, "id": None})) # <-- short circut and send a standard error. No need to even put
                self.transport.close()
                return 

            if isinstance(data, list): # run a batch
                task = self.loop.create_task(self.app.handle_batch_request(data))
            else: # run single req
                task = self.loop.create_task(self.app.handle_single_request(data))
            task.add_done_callback(self._task_response_callback)    


        def _task_response_callback(self, task):
            data = task.result()  # <-- returns from app.handle_request     
            if data is not None:
                logger.info(' ----> (~‾▿‾)~  Sending: {!r}'.format(data))
                data = self._encode_response(data)  # <--- json should not be malformed at this point. 
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

    def __init__(self, application):
        self._application = application 

    @property 
    def application(self):
        return self._application


    def listen(self, addr='127.0.0.1', port=6666):
        """ Listen on the specified addr and port
        """

        loop = asyncio.get_event_loop()
        coro = loop.create_server(lambda: self._JSONRPCProtocol(self._application), addr, port)
        server = loop.run_until_complete(coro)

        # Serve requests until Ctrl+C is pressed
        logger.info(' ----> ฅ^•ﻌ•^ฅ Serving on {} with {} threads...'.format(
            server.sockets[0].getsockname(), self._application.threadpool_max_workers))
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        # Close the server
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
