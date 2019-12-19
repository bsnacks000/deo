import asyncio 
import signal
import functools

from aiohttp import web

from .registry import EntrypointRegistry 
from .schemas import ContextData 
from .parsers import JSONByteParser

from . import exceptions

import logging 
import traceback
import coloredlogs
logger = logging.getLogger('asyncio')
coloredlogs.install(level=logging.INFO)


import concurrent.futures
import multiprocessing
_cpu_count = multiprocessing.cpu_count()

import marshmallow as ma

from distributed import Client


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
                return await err.contextdata_handler.dump_data(app.processpool_executor, app.loop)
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

        self._dask_scheduler_address = None
        self._dask_client = None
        

    @property 
    def entrypoint(self):
        """ shortcut used to wrap an entrypoint coroutine 
        """
        return self._entrypoint_registry.register


    def connect_to_dask(self, scheduler_address, **client_kwargs):
        """ connect to dask scheduler given a remote address. This must be running on another process.
        This creates a normal blocking client. To use the blocking API care must be taken to use the client in either 
        a thread or process. To use the async API in a coro you must pass asynchronous=True in the method call to submit or map.          
        A convenience method is provided on the application to make this call.
        """
        self._dask_scheduler_address = scheduler_address
        self._dask_client = Client(address=scheduler_address, **client_kwargs)
        logger.info('Connected to dask scheduler {}'.format(scheduler_address))


    @property 
    def dask_scheduler_address(self):
        if self._dask_scheduler_address is None:
            raise AttributeError('A dask remote scheduler address has not been set on this application')
        return self._dask_scheduler_address


    @property 
    def dask_client(self):
        """ request 
        """
        if self._dask_client is None:
            raise AttributeError('Dask client was not properly initialized')
        return self._dask_async_client


    async def run_on_thread(self, func, *args):
        """ A convenience coro to run any blocking callable on the app threadpool.
        """
        return await self._loop.run_on_executor(self._threadpool_executor, func, *args)


    async def run_on_process(self, func, *args):
        """ A convenince coro to run any blocking callable on the app processpool.
        """
        return await self._loop.run_on_executor(self._processpool_executor, func, *args)


    async def submit_on_dask(self, func, *args):
        """ A convenience coro to run any blocking callable using the dask async client. Call can 
        be either submit or map.
        """ 
        if self._dask_client is None:
            raise AttributeError('Dask client was not properly initialized')

        f = self._dask_client.submit(func, *args)
        return await self._dask_client.gather(f, asynchronous=True)


    async def map_on_dask(self, func, *args):

        if self._dask_client is None:
            raise AttributeError('Dask client was not properly initialized')
        f = self._dask_client.map(func, *args)
        return await self._dask_client.gather(f, asynchronous=True)


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
            raise AttributeError('An Application needs to be instantiated')
        return cls.__singleton


    @property
    def loop(self):
        if self._loop is None:
            raise AttributeError('A running loop needs to be set on this application')
        return self._loop
    

    def set_loop(self, loop):
        """ Set the running loop on the instance
        """
        self._loop = loop
    

    def _raise_rpc_error(self, rpc_exc, original_exc=None, contextdata_handler=None):
        """ This assures that errors are logged and that a contextdata_handler is bound to the exception. 
        We raise the the rpc_exc from the original here. 
        """
        logger.error(rpc_exc.message)
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
            await contextdata_handler.load_entrypoint(entry, data, self._processpool_executor, self._loop)  # <-- requires process

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
            self._raise_rpc_error(exc, err, contextdata_handler)

        except ma.ValidationError as err:
            exc = exceptions.InvalidRequest()
            self._raise_rpc_error(exc, err, contextdata_handler)

        except Exception as err: 
            exc = exceptions.InternalError()
            self._raise_rpc_error(exc, err, contextdata_handler)


    async def _handle_batch_request(self, parsed_data):
        """ handles a batch request by calling _handle single request as a TaskGroup. 
        """
        return await asyncio.gather(*[self._handle_single_request(d) for d in parsed_data])
    

    @rpc_error_handler
    async def _parse_in_executor(self, data, parser_cb):
        """ Except data and a byte parser
        """
        try:
            return await self._loop.run_in_executor(self._threadpool_executor, parser_cb, data)
        except exceptions.ParseError as err:
            self._raise_rpc_error(err)
    

    @rpc_error_handler
    async def _dispatch_parsed_data(self, parsed_data):
        if isinstance(parsed_data, list):  # <-- batch handle
            if len(parsed_data) == 0:
                exc = exceptions.ParseError()
                await self._raise_rpc_error(exc)
            return await self._handle_batch_request(parsed_data)
        else:
            return await self._handle_single_request(parsed_data)


    async def handle(self, data):
        """ parses data and inspects the result. Decides if the request is a single or batch 
        and calls the neccesary coros. All encoding is handled in the public handle method to 
        assure that potentially ParseError's are caught. #NOTE should always return...
        """         
        parsed_data = await self._parse_in_executor(data, self._parser.decode) #self._loop.run_in_executor(self._threadpool_executor, self._parser.decode, data)
        res = await self._dispatch_parsed_data(parsed_data)
        return await self._parse_in_executor(res, self._parser.encode)




# A HTTP POST request MUST specify the following headers:

# Content-Type: MUST be application/json.
# Content-Length: MUST contain the correct length according to the HTTP-specification.
# Accept: MUST be application/json.
# Of course, additional HTTP-features and -headers (e.g. Authorization) can be used.

# The Request itself is carried in the body of the HTTP message.

# Example:

# POST /myservice HTTP/1.1
# Host: rpc.example.com
# Content-Type: application/json
# Content-Length: ...
# Accept: application/json

# {
#     "jsonrpc": "2.0",
#     "method": "sum",
#     "params": { "b": 34, "c": 56, "a": 12 },
#     "id": 123
# }


# NOTE add optional headers accept_encoding

import time 
import zlib

# I'm messing around with the max client_bytes to allow for bigger data... 3MB max but
# with correct compression, data shouldn't need to get bigger.
ONE_MB = 1024 ** 2
THREE_MB = ONE_MB * 3


class ApplicationServer(object):
    """ Wrap a low-level aiohttp server and handle requests over http. Makes everyone's
    live alot easier.
    """

    def __init__(self, application, dask_scheduler_address=None):
        self._application = application
        self._dask_scheduler_address = dask_scheduler_address


    def _setup_application(self):
        pass



    async def serve(self, host, port):
        """ The main business logic for the server and handler. Attempts to 
        run the request. Will return empty body for 
        """

        async def _handler(request):
            if not request.can_read_body:
                return web.Response(status=400)

            if request.method != 'POST':
                return web.Response(status=405)    
            
            if request.headers['Content-Type'] != 'application/json':
                return web.Response(status=415)

            request._client_max_size = THREE_MB  # <--- its hacky but should be done elsewhere            
            
            # print(request._client_max_size)
            data = await request.read()  # just read the bytes. we defer parsing to the application
            result = await self._application.handle(data)

            if result is None: # <--- a successful notification - no id 
                return web.Response(status=204)
            
            #return web.Response(body=result, status=200)
            response = web.Response(
                body=result, 
                status=200, 
                zlib_executor_size=ONE_MB,
                zlib_executor=self._application.threadpool_executor)

            response.enable_compression()
            return response


        server = web.Server(_handler)
        runner = web.ServerRunner(server)
        await runner.setup()

        site = web.TCPSite(runner, host, port)
        await site.start()

        logger.info('Serving on http://{}:{}'.format(host, port))
        while True:
            await asyncio.sleep(100*3600)


    def listen(self, host='127.0.0.1', port=6666):
        
        loop = asyncio.get_event_loop()
        self._application.set_loop(loop)
        try:
            loop.run_until_complete(self.serve(host, port))
            loop.run_forever()
        except KeyboardInterrupt:
            pass  
        
        logger.warn('Shutting down...')
        self._application.threadpool_executor.shutdown(wait=True)
        self._application.processpool_executor.shutdown(wait=True)
        time.sleep(5)
        loop.close()


# The HTTP response MUST specify the following headers:

# Content-Type: MUST be application/json
# Content-Length: MUST contain the correct length according to the HTTP-specification.
# The status code SHOULD be:

# 200 OK
# for responses (both for Response and Error objects)

# 204 No Response / 202 Accepted
# for empty responses, i.e. as response to a Notification

# 307 Temporary Redirect / 308 Permanent Redirect
# for HTTP-redirections (note that the request may not be automatically retransmitted)

# 405 Method Not Allowed
# if HTTP GET is not supported: for all HTTP GET requests
# if HTTP GET is supported: if the client tries to call a non-safe/non-indempotent method via HTTP GET

# 415 Unsupported Media Type
# if the Content-Type is not application/json
# others
# for HTTP-errors or HTTP-features outside of the scope of JSON-RPC
# The Response (both on success and error) is carried in the HTTP body.