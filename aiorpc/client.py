""" Low and High level client implementations.
"""
import requests
import uuid
import json
import zlib
import gzip

import logging 
logger = logging.getLogger(__name__)

import sys


class JsonRPCHttpClient(object):
    """
    """

    _base = {
        'jsonrpc': "2.0",
        'id': None, 
        'method': None
    } 

    def __init__(self, addr='127.0.0.1', port=6666, timeout=5, use_base=True, ssl=False, client_max_size=1024**2*3):
        self.timeout = timeout 
        self.use_base = use_base 
        httpproto = 'https://' if ssl else 'http://'
        if port is not None:
            port = str(port)
        addrport = addr + ':' + port + '/' if port is not None else addr + '/'
        self.url = httpproto + addrport
        self.client_max_size = client_max_size

    
    def _send_req(self, data, raise_for_status, return_obj):
        data = json.dumps(data)
        sizeofdata = sys.getsizeof(data)

        headers = {'content-type': 'application/json', 'Accept': 'text/plain'}
        
        if sizeofdata > self.client_max_size // 3:   # <--- this allows gzip compression
            headers['accept-encoding'] = 'gzip, deflate'
            headers['content-encoding'] = 'gzip, deflate'
            data = gzip.compress(data.encode())

        response = requests.post(self.url, data=data, headers=headers)

        if raise_for_status:
            response.raise_for_status()
        try:
            if return_obj:
                response = response.json()
            return response
        except json.JSONDecodeError as err:
            logger.error('JSON-Decode Failed')
            return {'error': 'json-decode-failed', 'response': response, 'status_code': response.status_code}
        



    def send(self, method='', params={}, with_id=True, raise_for_status=False, return_obj=True):
        reqdata = self._base.copy()
        reqdata['method'] = method
        reqdata['params'] = params     

        if with_id:
            reqdata['id'] = uuid.uuid4().hex 
        
        return self._send_req(reqdata, raise_for_status, return_obj)


    def send_batch(self, methods=[], params=[], with_id=True, raise_for_status=False,return_obj=True):
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
                reqdata['id'] = uuid.uuid4().hex
            batch.append(reqdata)

        return self._send_req(batch, raise_for_status,return_obj)
