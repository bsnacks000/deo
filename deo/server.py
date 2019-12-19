import time 
import zlib

import aiohttp
from aiohttp import web
import asyncio

import logging 
logger = logging.getLogger('asyncio')

#<CIMultiDictProxy('Host': '127.0.0.1:65432', 'Connection': 'Upgrade', 'Pragma': 'no-cache', 'Cache-Control': 'no-cache', 'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/78.0.3904.108 Chrome/78.0.3904.108 Safari/537.36', 'Upgrade': 'websocket', 'Origin': 'chrome-search://local-ntp', 'Sec-WebSocket-Version': '13', 'Accept-Encoding': 'gzip, deflate, br', 'Accept-Language': 'en-US,en;q=0.9', 'Sec-WebSocket-Key': 'MhBH8oyXhNtw0tz2e8ITbA==', 'Sec-WebSocket-Extensions': 'permessage-deflate; client_max_window_bits')>
ONE_MB = 1024**2 
HALF_MB = ONE_MB // 2

TWO_MB = 1024**2 * 2
FIVE_MB = 1024**2 * 5
TEN_MB = 1024**2 * 10

class ApplicationServer(object):
    """ wrap our rpc implementation in an aiohttp application. Should be able to serve both http and websockets.
    """

    def __init__(self, application):
        self._application = application

    def _make_app(self):
        
        # XXX ws needs to be 
        async def websocket_handler(request):
            ws = web.WebSocketResponse(max_msg_size=FIVE_MB) # NOTE automatically compresses responses
            await ws.prepare(request)

            async for msg in ws:                
                if msg.type == aiohttp.WSMsgType.TEXT:
                    result = await self._application.handle(msg.data)
                    if result is None: 
                        continue # according to spec notifications do not require a response so we don't.
                    await ws.send_str(result)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    logging.error('ws connection closed with exception %s' % ws.exception())
            return ws

        async def http_handler(request):
            
            if not request.can_read_body:
                return web.Response(status=400)

            if request.headers['Content-Type'] != 'application/json':
                return web.Response(status=415)
            
            request._client_max_size = TWO_MB

            data = await request.read()
            result = await self._application.handle(data)

            if result is None: # <--- a successful notification - no id but we must return since http
                return web.Response(status=204)
            
            response = web.Response(  # return response compress if larger then 0.5 MB
                text=result, 
                status=200, 
                zlib_executor_size=HALF_MB,
                zlib_executor=self._application.threadpool_executor)

            response.enable_compression()

            return response

        app = web.Application()
        app.router.add_get('/ws/', websocket_handler)
        app.router.add_post('/', http_handler)
        
        return app

    def listen(self, host='127.0.0.1', port=6666):

        loop = asyncio.get_event_loop()
        self._application.set_loop(loop)
        
        app = self._make_app()
        web.run_app(app, host=host, port=port)










# I'm messing around with the max client_bytes to allow for bigger data... 3MB max but
# with correct compression, data shouldn't need to get bigger.

# ONE_MB = 1024 ** 2
# THREE_MB = ONE_MB * 3


# class ApplicationServer(object):
#     """ Wrap a low-level aiohttp server and handle requests over http. Makes everyone's
#     live alot easier.
#     """

#     def __init__(self, application, dask_scheduler_address=None):
#         self._application = application
#         self._dask_scheduler_address = dask_scheduler_address


#     def _setup_application(self):
#         pass



#     async def serve(self, host, port):
#         """ The main business logic for the server and handler. Attempts to 
#         run the request. Will return empty body for 
#         """

#         async def _handler(request):
#             if not request.can_read_body:
#                 return web.Response(status=400)

#             if request.method != 'POST':
#                 return web.Response(status=405)    
            
#             if request.headers['Content-Type'] != 'application/json':
#                 return web.Response(status=415)

#             request._client_max_size = THREE_MB  # <--- its hacky but should be done elsewhere            
            
#             # print(request._client_max_size)
#             data = await request.read()  # just read the bytes. we defer parsing to the application
#             result = await self._application.handle(data)

#             if result is None: # <--- a successful notification - no id 
#                 return web.Response(status=204)
            
#             #return web.Response(body=result, status=200)
#             response = web.Response(
#                 body=result, 
#                 status=200, 
#                 zlib_executor_size=ONE_MB,
#                 zlib_executor=self._application.threadpool_executor)

#             response.enable_compression()
#             return response


#         server = web.Server(_handler)
#         runner = web.ServerRunner(server)
#         await runner.setup()

#         site = web.TCPSite(runner, host, port)
#         await site.start()

#         logger.info('Serving on http://{}:{}'.format(host, port))
#         while True:
#             await asyncio.sleep(100*3600)


#     def listen(self, host='127.0.0.1', port=6666):
        
#         loop = asyncio.get_event_loop()
#         self._application.set_loop(loop)
#         try:
#             loop.run_until_complete(self.serve(host, port))
#             loop.run_forever()
#         except KeyboardInterrupt:
#             pass  
        
#         logger.warn('Shutting down...')
#         self._application.threadpool_executor.shutdown(wait=True)
#         self._application.processpool_executor.shutdown(wait=True)
#         time.sleep(5)
#         loop.close()


# A HTTP POST request MUST specify the following headers:

# Content-Type: MUST be application/json.
# Content-Length: MUST contain the correct length according to the HTTP-specification.
# Accept: MUST be application/json.
# Of course, additional HTTP-features and -headers (e.g. Authorization) can be used.

# The Request itself is carried in the body of the HTTP message.

# Example:

# POST /myservice HTTP/1.1
# Host: rpc.example.com
# Content-Type: application/json
# Content-Length: ...
# Accept: application/json

# {
#     "jsonrpc": "2.0",
#     "method": "sum",
#     "params": { "b": 34, "c": 56, "a": 12 },
#     "id": 123
# }


# The HTTP response MUST specify the following headers:

# Content-Type: MUST be application/json
# Content-Length: MUST contain the correct length according to the HTTP-specification.
# The status code SHOULD be:

# 200 OK
# for responses (both for Response and Error objects)

# 204 No Response / 202 Accepted
# for empty responses, i.e. as response to a Notification

# 307 Temporary Redirect / 308 Permanent Redirect
# for HTTP-redirections (note that the request may not be automatically retransmitted)

# 405 Method Not Allowed
# if HTTP GET is not supported: for all HTTP GET requests
# if HTTP GET is supported: if the client tries to call a non-safe/non-indempotent method via HTTP GET

# 415 Unsupported Media Type
# if the Content-Type is not application/json
# others
# for HTTP-errors or HTTP-features outside of the scope of JSON-RPC
# The Response (both on success and error) is carried in the HTTP body.