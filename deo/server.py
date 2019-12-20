import time 
import zlib

import aiohttp
from aiohttp import web
import asyncio

import logging 
logger = logging.getLogger('asyncio')

ONE_MB = 1024**2 
HALF_MB = ONE_MB // 2

TWO_MB = 1024**2 * 2
FIVE_MB = 1024**2 * 5
TEN_MB = 1024**2 * 10



async def websocket_handler(request, deo_application):
    """ The default websocket handler.
    """
    
    ws = web.WebSocketResponse(max_msg_size=FIVE_MB) # NOTE automatically compresses responses
    await ws.prepare(request)

    async for msg in ws:                
        if msg.type == aiohttp.WSMsgType.TEXT:
            result = await deo_application.handle(msg.data)
            if result is None: 
                continue # according to spec notifications do not require a response so we don't.
            await ws.send_str(result)
    
        elif msg.type == aiohttp.WSMsgType.ERROR:
            logging.error('ws connection closed with exception %s' % ws.exception())
    return ws


async def http_handler(request, deo_application):
    """ The default http_handler.
    """

    if not request.can_read_body:
        return web.Response(status=400)

    if request.headers['Content-Type'] != 'application/json':
        return web.Response(status=415)
    
    request._client_max_size = TWO_MB

    data = await request.read()
    result = await deo_application.handle(data)

    if result is None: # <--- a successful notification - no id but we must return since http
        return web.Response(status=204)
    
    response = web.Response(  # return response compress if larger then 0.5 MB
        text=result, 
        status=200, 
        zlib_executor_size=HALF_MB,
        zlib_executor=self._application.threadpool_executor)

    response.enable_compression()

    return response


class ApplicationServer(object):
    """ wrap our rpc implementation in an aiohttp application. Should be able to serve both http and websockets.
    The purpose of deo is to handle large request sizes (time series data) so we take some liberties with client max size etc.
    """

    def __init__(self, application, websocket_handler=websocket_handler, http_handler=http_handler):
        self._application = application
        self._websocket_handler = websocket_handler 
        self._http_handler = http_handler


    def make_app(self):
        """ An application factory. Builds an http server using aiohttp with two root level connections for 
        websocket and regular http post requests.
        """
        
        async def websocket_handler(request):
            return await self._websocket_handler(request, self._application)

        async def http_handler(request):
            return await self._http_handler(request, self._application)
            
        app = web.Application()
        app.router.add_get('/ws/', websocket_handler)
        app.router.add_post('/', http_handler)

        app = self.extend_app(app)
        return app


    def extend_app(self, app):
        """ A hook for extending the aiohttp app with extra handlers, middleware etc...
        """
        return app


    def listen(self, host='127.0.0.1', port=65432):
        """ Easiest way to run the app. Aiohttp takes care of the annoying asyncio shutdown logic. 
        """
        
        loop = asyncio.get_event_loop()
        self._application.set_loop(loop)
        
        app = self.make_app()
        web.run_app(app, host=host, port=port)
