""" sync and async clients 
"""
import socket 
import sys 
import orjson 
import uuid


class Client(object):

    _base = {
        'jsonrpc': "2.0",
        'id': None, 
        'method': None
    }

    def __init__(self, addr='127.0.0.1', port=6666, max_bytes=1048576):
        self.addr = addr 
        self.port = port  
        self.max_bytes = max_bytes


    def send(self, method='', params={}):

        req = self._base.copy()
        req['method'] = method 
        req['params'] = params 
        req['id'] = uuid.uuid4().hex        
        
        bjson = orjson.dumps(req)
        #print(req)
        buflen = 0

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect((self.addr, self.port))
            sock.sendall(bjson)
            data = sock.recv(self.max_bytes)
        
        return orjson.loads(data)
        
