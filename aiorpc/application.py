# import curio 
import asyncio 

from .registry import EntrypointRegistry 
from .schemas import ContextData 
from .parsers import JSONByteParser

from . import exceptions
import functools

# import logging 
# logger = logging.getLogger(__name__)
# # XXX <---- replace with aiologger...

import aiologger


import concurrent.futures
import multiprocessing
_cpu_count = multiprocessing.cpu_count()

import marshmallow as ma
import traceback


def rpc_error_handler(method):
    """ A decorator that handles the logic of reporting the various RPC errors. It will handle dumping the 
    correct schema if a contextdata_handler is present and active. If not then it will construct its own message. 
    It returns the response as a dictionary but does not serialize it. 
    """
    @functools.wraps(method)
    async def _inner(*args, **kwargs):
        try:
            return await method(*args, **kwargs)

        except exceptions.AiorpcException as err: 
            # We check the state of the contextdata_handler... If it was correctly initialized we can write the detail
            # if not then we try to extract the id from the raw data packet and return 
            id_ = None
            detail = {'code': err.error_code, 'message': err.message, 'data':{'detail': traceback.format_exc()}}  

            if err.contextdata_handler.is_initialized():
                err.contextdata_handler.write_to_error(detail) # we write this detail to the error field.     
                app = Application.current_app() # <-- get the active application 
                return await err.contextdata_handler.dump_data(app.processpool_executor, app.current_loop)
            else:
                data = err.contextdata_handler.data if err.contextdata_handler.data is not None else {} 
                id_ = data['id'] if 'id' in data else None 
            
            return {"jsonrpc": "2.0", "error": {"code": err.error_code, "message": err.message, 'data':{'detail': traceback.format_exc() }}, "id": id_}    
    return _inner 




class ContextDataHandler(object):
    """ A helper class that uses the schema to create a ContextData object 
    that params and results can be read/written to. We defer data initialization 
    so that we can still reference this object during error handling regardless of 
    whether data was given. 
    """

    def __init__(self):
        self._data = None 
        self._schema = None 
        self._contextdata = None  

    @property 
    def contextdata(self):
        return self._contextdata 

    @property
    def data(self):
        return self._data


    def is_initialized(self):
        """ If we have contextdata then we consider the 
        object initialized.
        """
        if self._contextdata:
            return True 
        return False 


    def load_data(self, data):
        self._data = data


    async def load_entrypoint(self, entry, data, executor, loop):
        """ Allows us to lazily initialize the object. Pass in an 
        executor to  
        """
        self._data = data 
        self._schema = entry.schema_class() 
        self._contextdata = await loop.run_in_executor(
            executor, self._schema.load, self._data)

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

    async def dump_data(self, executor, loop):
        obj = await loop.run_in_executor(
            executor, self._schema.dump, self._contextdata)

        if 'error' in obj:
            if len(obj['error']) == 0:
                del obj['error']
        elif 'result' in obj:
            if len(obj['result']) == 0:
                del obj['result']
        return obj


def run_on_thread(func):
    app = Application.current_app() 
    async def _wrapped_coro(*args, **kwargs):
        f = functools.partial(func, *args, **kwargs)
        return await app.current_loop.run_in_executor(app.threadpool_executor, f)
    return _wrapped_coro



class Application(object):
    """ An application object. 
    """
    __singleton = None  
    

    def __new__(cls, *args, **kwargs):
        if not cls.__singleton:
            cls.__singleton = super().__new__(cls, *args, **kwargs)
        return cls.__singleton


    def __init__(self):
        self._entrypoint_registry = EntrypointRegistry() 
        self._parser = JSONByteParser()
        self._loop = None   # initialized just in time via handle method

        # We keep these running... can be used for entrypoints but also for internals (deserialization/parsing) 
        self._processpool_executor = concurrent.futures.ProcessPoolExecutor(max_workers=_cpu_count)
        self._threadpool_executor = concurrent.futures.ThreadPoolExecutor(max_workers=_cpu_count * 2 + 3)


    @property 
    def entrypoint(self):
        """ shortcut used to wrap an entrypoint coroutine 
        """
        return self._entrypoint_registry.register


    @property 
    def threadpool_executor(self):
        return self._threadpool_executor 

    @property 
    def processpool_executor(self):
        return self._processpool_executor


    @classmethod 
    def current_app(cls):
        """ Can be used to access the Application singleton. Will fail if no app has been initialized.
        """
        if cls.__singleton is None:
            raise ValueError('An Application needs to be instantiated')
        return cls.__singleton


    @property 
    def current_loop(self):
        """ Returns the active loop. Will fail if no loop has been initialized
        """
        if self._loop is None:
            raise ValueError('An event loop has not yet been set on the Application instance')
        return self._loop
        
    

    async def _handle_rpc_error(self, rpc_exc, original_exc=None, contextdata_handler=None):
        """ This assures that errors are logged and that a contextdata_handler is bound to the exception. 
        We raise the the rpc_exc from the original here. 
        """
        logger = aiologger.Logger.with_default_handlers(name=__name__)

        await logger.error(rpc_exc.message)
        if not contextdata_handler:
            contextdata_handler = ContextDataHandler()

        rpc_exc.contextdata_handler = contextdata_handler
        if original_exc:
            raise rpc_exc from original_exc 
        else:
            raise rpc_exc


    @rpc_error_handler
    async def _handle_single_request(self, data):
        """ The complete logic for a single request.  All blocking internal APIs need to be 
        awaited here. 
        """
        contextdata_handler = ContextDataHandler()

        try:
            entry = self._entrypoint_registry.get_entrypoint(data['method']) 
            await contextdata_handler.load_entrypoint(entry, data, self._processpool_executor, self._loop)

            params = contextdata_handler.get_params()
            
            if params is None:
                res = await entry.func()
            elif isinstance(params, (list, tuple)):
                res = await entry.func(*params)
            else:
                res = await entry.func(**params)
        
            contextdata_handler.write_to_result(res)
            if contextdata_handler.get_id() is not None:
                return await contextdata_handler.dump_data(self._processpool_executor, self._loop)

        except exceptions.RegistryEntryError as err:
            exc = exceptions.MethodNotFound()
            await self._handle_rpc_error(exc, err, contextdata_handler)

        except ma.ValidationError as err:
            exc = exceptions.InvalidRequest()
            await self._handle_rpc_error(exc, err, contextdata_handler)

        except Exception as err: 
            exc = exceptions.InternalError()
            await self._handle_rpc_error(exc, err, contextdata_handler)


    async def _handle_batch_request(self, parsed_data):
        """ handles a batch request by calling _handle single request as a TaskGroup. 
        """
        return await asyncio.gather(*[self._handle_single_request(d) for d in parsed_data])
        

    @rpc_error_handler
    async def _handle(self, data):
        """ parses data and inspects the result. Decides if the request is a single or batch 
        and calls the neccesary coros. All encoding is handled in the public handle method to 
        assure that potentially ParseError's are caught. 
        """
        try:
            parsed_data = await self._loop.run_in_executor(self._threadpool_executor, self._parser.decode, data)  # prob fast enough for a thread

        except exceptions.ParseError as err:
            await self._handle_rpc_error(err)

        if isinstance(parsed_data, list):  # <-- batch handle
            if len(data) == 0:
                exc = exceptions.ParseError()
                await self._handle_rpc_error(exc)
            return await self._handle_batch_request(parsed_data)
        else:
            return await self._handle_single_request(parsed_data)
        
        
    async def handle(self, data):
        """ we need to nest the decoding logic. Encoding should never fail. This assures that if 
        a ParseError occurs it is correctly encoded in bytes before being sent.
        """
        if self._loop is None: # XXX possibly set elsewhere, though this will work...
            self._loop = asyncio.get_running_loop()
        
        result = await self._handle(data)
        return await self._loop.run_in_executor(self._threadpool_executor, self._parser.encode, result) 