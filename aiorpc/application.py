""" 
"""
import asyncio
from .parsers import JSONByteParser
from .registry import EntrypointRegistry 

import concurrent.futures 
import logging 
import functools

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ContextDataHandler(object):
    """ used internally by methodology to handle the contextdata object. Proxies many calls to
    the contexted data interface and has access to the contextdata's related schema.
    """

    def __init__(self, data, schema_class, **schema_kwargs):
        self._schema = schema_class(**schema_kwargs)
        self._contextdata = self._schema.load(data)


    @property
    def contextdata(self):
        return self._contextdata


    def write_to_result(self, result):
        """ Assign result object onto contextdata
        """
        self._contextdata.result = result


    def write_to_error(self, err):
        """ Assign an error object to the context
        """
        self._contextdata.error = err


    def dump_data(self, partial=False):
        if partial:
            # we reinstantiate with a fresh schema class set to parial # XXX not 100% sure if this will work correctly
            # we basically want to ignore validation for missing response data in cases where an error occurred, but validate what
            # response data we managed to already set in the call_chain.
            schema = self._schema.__class__(partial=partial)
            return schema.dump(self._contextdata)
        return self._schema.dump(self._contextdata) 


class Application(object):
    """ The application object handles the entrypoint registry and manages the event loop. 
    An instance is passed to the TCPServer protocol. 

    TODO methods for handling dask client. 
    """

    def __init__(self, addr='127.0.0.1', port=6666, threadpool_max_workers=5):
        self.addr = addr 
        self.port = port 
        self._entrypoint_registry = EntrypointRegistry()
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=threadpool_max_workers)
        self.loop = asyncio.get_event_loop()

    @property 
    def entrypoint(self):
        return self._entrypoint_registry

    # TODO << this is not finished... error handling needed. All errors should map to an error object call with RPC errors.
    def _run_in_executor(self, raw_request):
        """ Blocking calls happen in a threadpool executor
        """
        entry = self._entrypoint_registry.get_entrypoint(raw_request['method'])  # <-- XXX error handling 
        
        contextdata_handler = ContextDataHandler(raw_request, entry.schema_class)   # <--- XXX error on load must be handled
        params = contextdata_handler.contextdata.params

        # TODO <- check params against entry.args/kwargs 
        partial = False 

        try:
            if isinstance(params, list):    # <---- XXX runtime error handling  
                res = entry.func(*params)
            else:
                res = entry.func(**params)
            #print(res)
            contextdata_handler.write_to_result(res)
        #except ChainWrapperError as err <-------XXX preserve data from error chain (detail)
        except Exception as err: 
            partial = True 
            contextdata_handler.write_to_error({'error': 'obj'})  # XXX todo 
            raise 
        
        finally: 
            #print(contextdata_handler.contextdata.result)
            return contextdata_handler.dump_data(partial=partial)   

    async def handle_request(self, raw_request):
        """ Parse the method from the raw request object, create the context data and run. If its a list we run 
        all the rquests and await the responses.
        """
        if isinstance(raw_request, list):
            futures = [self.loop.run_in_executor(self._executor, self._run_in_executor, req) for req in raw_request]        
            return await asyncio.gather(*futures)
        else:
            return await self.loop.run_in_executor(self._executor, self._run_in_executor, raw_request)



class _JSONRPCProtocol(asyncio.Protocol):

    def __init__(self, app): 
        self.app = app 
        self.parser = JSONByteParser()
        
    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        logger.info(' <----- Connection from {}'.format(peername))
        self.transport = transport

    
    def data_received(self, data):
        raw_req = self.parser.decode(data)
        logger.info(' <---- Data received: {!r}'.format(raw_req))

        task = self.app.loop.create_task(self.app.handle_request(raw_req))
        task.add_done_callback(self._task_response_callback)    
    
    def _task_response_callback(self, task):
        data = task.result()  # <-- returns from app.handle_request

        data = self.parser.encode(data)
        logger.info(' ----> Sending: {!r}'.format(data))

        self.transport.write(data)

        logger.info(' ----> Closing Connection:')
        self.transport.close()



class TCPServer(object):
    """ Simple TCPServer that runs the application.
    """

    def __init__(self, app):
        self.app = app 


    def listen(self):

        loop = self.app.loop
        coro = loop.create_server(lambda: _JSONRPCProtocol(self.app), self.app.addr, self.app.port)
        server = loop.run_until_complete(coro)

        # Serve requests until Ctrl+C is pressed
        logger.info(' ----> Serving on {}'.format(server.sockets[0].getsockname()))
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        # Close the server
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
