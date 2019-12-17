""" sync and async clients 
"""
import socket 
import sys 
import orjson 
import uuid


class Client(object):
    """ A simple blocking JSON-RPC2 TCP client that can send both single and batch requests to a running server. 
    """

    _base = {
        'jsonrpc': "2.0",
        'id': None, 
        'method': None
    }

    def __init__(self, addr='127.0.0.1', port=6666, max_bytes=1048576, connect_timeout=5, recv_timeout=20):
        self.addr = addr 
        self.port = port  
        self.max_bytes = max_bytes
        self.connect_timeout = connect_timeout
        self.recv_timeout = recv_timeout

    def _send_req(self, req):
        bjson = orjson.dumps(req)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(self.connect_timeout)
            sock.connect((self.addr, self.port))
            sock.sendall(bjson)
            sock.settimeout(self.recv_timeout)
            data = sock.recv(self.max_bytes)
        
        return orjson.loads(data)


    def send(self, method='', params={}):
        req = self._base.copy()
        req['method'] = method 
        req['params'] = params 
        req['id'] = uuid.uuid4().hex        
        
        return self._send_req(req)
        

    def send_batch(self, methods=[], params=[]):
        """ zips lists of methods and parameters to create a batch request. 
        """
        if len(methods) != len(params):
            raise TypeError('`params` and `methods` must be the same length.')
        
        batch = []
        for i,m in enumerate(methods):
            req = self._base.copy()
            req['method'] = m 
            req['params'] = params[i]
            req['id'] = uuid.uuid4().hex
            batch.append(req)

        return self._send_req(batch)
