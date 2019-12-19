""" Exceptions mapped to error codes the JSON-rpc 2 spec. 

-32768 to -32000 are reserved for pre-defined errors

code: -32700  message: Parse Error  -> invalid json received by the server. Error occurred while parsing the json text
code: -32600  message: Invalid Request -> Json sent is not a valid Request object 
code: -32601  message: Method not Found -> The method does not exist or is not available  
code: -32602  message: Invalid Params -> Invalid method parameters  
code: -32603  message: Internal Error -> Internal JSON-RPC error 
code: -32000 -> -32099 message: Server Error -> Reserved for implementation defined server errors 

"""

# ----------------- Exceptions thrown by the rpc server

class DeoException(Exception):
    """ Defines a BaseException that contains a default message and JSON-rpc 2 error code """
    error_code = None 
    message = ''

    def __init__(self, *args, **kwargs):
        if not (args or kwargs):
            args = (self.message,)
        super().__init__(*args, **kwargs)


class ParseError(DeoException):
    error_code = -32700
    message = 'An error occured while parsing the JSON text.'


class InvalidRequest(DeoException):
    error_code = -32600
    message = 'Not a valid request object'


class MethodNotFound(DeoException):
    error_code = -32601 
    message = 'The requested method does not exist on this server.'


class InvalidParams(DeoException):
    error_code = -32602
    message = 'Invalid parameters passed to requested method.'


class InternalError(DeoException):
    error_code = -32603
    message = 'An internal error occurred.'


# ---- registry exception
class RegistryEntryError(KeyError, DeoException):
    """ Raised if the key is not present in the WorfklowRegistry"""

