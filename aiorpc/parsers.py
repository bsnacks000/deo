"""
"""
import orjson


class JSONByteParser(object):

    def encode(self, data):
        return orjson.dumps(data)

    def decode(self, data):
        return orjson.loads(data)
