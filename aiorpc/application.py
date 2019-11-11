""" 
"""
import asyncio
from .parsers import JSONByteParser
from .registry import Registry 

import concurrent.futures 


class ContextDataHandler(object):
    """ used internally by methodology to handle the contextdata object. Proxies many calls to
    the contexted data interface and has access to the contextdata's related schema.
    """

    def __init__(self, schema_class, **schema_kwargs):
        self._schema = schema_class(**schema_kwargs)
        self._contextdata = None

    @property
    def contextdata(self):
        return self._contextdata


    def load_inputdata(self, data):
        self._contextdata = self._schema.load(data)


    def write_to_result(self, data):
        """ Assign the result to the context
        """
        self._contextdata.result = data 


    def write_to_error(self, err_obj):
        """ Assign an error object to the context
        """
        self._contextdata.error = error


    def dump_data(self, partial=False):
        if partial:
            # we reinstantiate with a fresh schema class set to parial # XXX not 100% sure if this will work correctly
            # we basically want to ignore validation for missing response data in cases where an error occurred, but validate what
            # response data we managed to already set in the call_chain.
            schema = self._schema.__class__(partial=partial)
            return schema.dump(self._contextdata)
        return self._schema.dump(self._contextdata) 




class Application(object):


    def __init__(self, addr='127.0.0.1', port=6666, threadpool_max_workers=5):
        self.addr = addr 
        self.port = port 
        self._registry = Registry()
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=threadpool_max_workers)

    

    def register(self, schema_name=''):
        """ A decorator that registers a method to the application.
        """
        self._registry.entrypoint(schema_name)
    

    def get_method_handler(self, raw_request):
        """ Parse the method from the raw request object and create the context data.
        """






class _JSONRPCProtocol(asyncio.Protocol):

    def __init__(self, app=None, loop=None): 
        self.parser = JSONByteParser()
        self.app = app 
        self.loop = loop 

    def connection_made(self, transport):
        peername = transport.get_extra_info('peername')
        print('Connection from {}'.format(peername))
        self.transport = transport

    
    def data_received(self, data):
        raw_req = self.parser.decode(data)
        print('Data received: {!r}'.format(req))

        # NOTE We want to handle in a coroutine like this 
        # task = loop.create_task(functools.partial(self.handle_request, req))
        # task.add_done_callback(self._task_response_callback)
        
        print('Send: {!r}'.format(req))
        self.transport.write(data)

        print('Close the client socket')
        self.transport.close()


    async def handle_single_request(self, req):
        # offload the actual business logic to a thread here via executor.
        # We have two different scenarios... if a batch request we need to run and then await as completed and return the array 
        # if its a single request we run once, await the result and then write out the single response
        # for each thing we need to: select method, createcontextdata, map context params to func, call function and map result 
        pass
    
    
    def _task_response_callback(self, task):
        # data = task.result()
        # ... presumably most error handling will happen here since we must guarantee a write.
        # ... transport.write and trasnport.close would be handled here 
        pass


class TCPServer(object):

    def run_server(self, loop,  app=None, addr='127.0.0.1', port=6666):

        coro = loop.create_server(lambda: _JSONRPCProtocol(app, loop), addr, port)
        server = loop.run_until_complete(coro)

        # Serve requests until Ctrl+C is pressed
        print('Serving on {}'.format(server.sockets[0].getsockname()))
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        # Close the server
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()
