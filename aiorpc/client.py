""" Low and High level client implementations.
"""
import requests
import uuid

class JsonRPCHttpClient(object):
    """
    """

    _base = {
        'jsonrpc': "2.0",
        'id': None, 
        'method': None
    } 

    def __init__(self, addr='127.0.0.1', port=6666, timeout=5, use_base=True, ssl=False):
        self.timeout = timeout 
        self.use_base = use_base 
        httpproto = 'https://' if ssl else 'http://'
        addrport = addr + ':' + port + '/' if port is not None else addr + '/'
        self.url = httpproto + addrport
    
    def _send_req(self, data):
        headers = {'content-type': 'application/json'}
        res = requests.post(self.url, json=data, headers=headers)

        if raise_for_status:
            res.raise_for_status()
        return res


    def send(self, method='', params={}, with_id=True, raise_for_status=False):
        reqdata = self._base.copy()
        reqdata['method'] = method
        reqdata['params'] = params     

        if with_id:
            reqdata['id'] = uuid.uuid().hex 
        
        return self._send_req(reqdata, raise_for_status)


    def send_batch(self, methods=[], params=[], raise_for_status=False):
        """ zips lists of methods and parameters to create a batch request. 
        """
        if len(methods) != len(params):
            raise TypeError('`params` and `methods` must be the same length.')
        
        batch = []
        for i,m in enumerate(methods):
            reqdata = self._base.copy()
            reqdata['method'] = m 
            reqdata['params'] = params[i]
            if with_id:
                reqdata['id'] = uuid.uuid().hex
            batch.append(reqdata)

        return self._send_req(batch, raise_for_status)
