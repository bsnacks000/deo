""" Request parsers needed.
"""
import rapidjson
from .exceptions import ParseError


class JSONByteParser(object):
    """ A simple class that wraps the JSON parsing and handles ParseError
    """

    def encode(self, data):
        try:
            return rapidjson.dumps(data)
        except rapidjson.JSONEncodeError as err:
            raise ParseError from err 

    def decode(self, data):
        try:
            return rapidjson.loads(data)
        except rapidjson.JSONDecodeError as err:
            raise ParseError from err  
