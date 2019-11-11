""" Exceptions for the chain API impl
"""


class ChainWrapperError(Exception):
    """ Wraps an exception thrown in the method call chain"""


class InvalidMethodError(Exception):
    """Thrown if the Chain is passed an invalid method call """


class JoinKeyError(KeyError):
    """ Thrown when a bad key is found"""


class ChainArgValueError(ValueError):
    """ thrown if arguments in the chained function are misaligned """

