# ----------- Exceptions thrown by the Chain API
from ..exceptions import AiorpcException


class ChainError(Exception):
    """ base class for chain errors. Used to define a namespace"""


class ChainWrapperError(AiorpcException, ChainError):
    """ Wraps an exception thrown in the method call chain"""
    error_code = -32001
    message = 'An error occurred in the while running the chain'


class InvalidMethodError(AiorpcException, ChainError):
    """Thrown if the Chain is passed an invalid method call """
    error_code = -32002
    message = 'An Invalid method was called from the chain'


class ChainArgValueError(AiorpcException, ChainError):
    """ thrown if arguments in the chained function are misaligned """
    error_code = -32003
    message = 'A function in the chain was not passed the correct argument.'