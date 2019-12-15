""" Request parsers needed.
"""
import orjson

from .exceptions import ParseError


class JSONByteParser(object):
    """ A simple class that wraps the JSON parsing and handles ParseError
    """

    def encode(self, data):
        try:
            return orjson.dumps(data)
        except orjson.JSONEncodeError as err:
            raise ParseError from err 

    def decode(self, data):
        try:
            return orjson.loads(data)
        except orjson.JSONDecodeError as err:
            raise ParseError from err  
