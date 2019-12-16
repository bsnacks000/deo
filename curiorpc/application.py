import curio 

from .registry import EntrypointRegistry 
from .schemas import ContextData 
from .parsers import JSONByteParser

from . import exceptions
import functools

import logging 
logger = logging.getLogger(__name__)

def rpc_error_handler(method):
    """ A decorator that handles the logic of reporting the various RPC errors.
    """
    @functools.wraps(method)
    async def _inner(*args, **kwargs):
        try:
            return await method(*args, **kwargs)

        except exceptions.CuriorpcException as err: 
            # We check the state of the contextdata_handler... If it was correctly initialized we can write the detail
            # if not then we try to extract the id from the raw data packet and return 
            id_ = None
            if err.contextdata_handler.is_initialized():
                detail = {
                    'code': err.error_code, 
                    'message': err.message, 
                    'data':{'detail': traceback.format_exc()}}    
                err.contextdata_handler.write_to_error(detail) # we write this detail to the error field.     
                return await curio.run_in_process(err.contextdata_handler.dump_data)
            else:
                data = err.contextdata_handler.data if err.contextdata_handler.data is not None else {} 
                id_ = data['id'] if 'id' in data else None 
            
            return {"jsonrpc": "2.0", "error": {"code": err.error_code, "message": err.message}, "id": id_}    

    return _inner 




class ContextDataHandler(object):

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


    def load_entrypoint(self, entry):
        """ Allows us to lazily initialize the object 
        """
        self._schema = entry.schema_class() 
        self._contextdata = self._schema.load(data)

    
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

    
    def _handle_rpc_error(self, rpc_exc, original_exc=None, contextdata_handler=None):
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

    @rpc_error_handler
    async def _handle_single_request(self, parsed_data):
        """ The complete logic for a single request.  All blocking internal APIs need to be 
        awaited here. 
        """
        print(parsed_data)
        return parsed_data


    async def _handle_batch_request(self, parsed_data):
        """ handles a batch request by calling _handle single request as a TaskGroup. 
        """
        print(parsed_data)
        return parsed_data


    @rpc_error_handler
    async def _handle(self, data):
        """ parses data and inspects the result. Decides if the request is a single or batch 
        and calls the neccesary coros. All encoding is handled in the public handle method to 
        assure that potentially ParseError's are caught. 
        """
        try:
            parsed_data = await curio.run_in_process(self._parser.decode, data)  # not sure if this is fast enough to run in a thread
        except exceptions.ParseError as err:
            self._handle_rpc_error(err)

        if isinstance(parsed_data, list):  # <-- batch handle
            if len(data) == 0:
                exc = exceptions.ParseError()
                self._handle_rpc_error(exc)
            return await self._handle_batch_request(parsed_data)
        else:
            return await self._handle_single_request(parsed_data)
        
        
    async def handle(self, data):
        """ we need to nest the decoding logic. Encoding should never fail. This assures that if 
        a ParseError occurs it is correctly encoded in bytes before being sent.
        """
        result = await self._handle(data)
        #print(result)
        return await curio.run_in_process(self._parser.encode, result)  # not sure if this is fast enough to run in a thread