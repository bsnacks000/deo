#import curio  
import asyncio 
import logging 

logger = logging.getLogger(__name__)


class TCPServer(object):
    """ Recieve up to max bytes via TCP and pass to application for processing.
    """

    def __init__(self, application, maxbytes=100000):
        self._application = application
        self.maxbytes = maxbytes

    async def listen(self, addr='127.0.0.1', port=6666):

        async def _app_handler(reader, writer):
            addr = writer.get_extra_info('peername')
            logger.info(' <----- ( •_•) Connection from {}'.format(addr))
            data = await reader.read(10000)

            res = await self._application.handle(data)
            
            writer.write(res)
            await writer.drain()
            
            logger.info(' ----> Closing Connection from {}:  (ツ)'.format(addr))
            writer.close()

        
        server = await asyncio.start_server(_app_handler, addr, port)
        async with server:
            logger.info(' ----> ฅ^•ﻌ•^ฅ Serving on {}:{}'.format(addr, port))
            await server.serve_forever()


    # async def app_handler(self, sock, addr):
    #     logger.info(' <----- ( •_•) Connection from {}'.format(addr))
    #     s = sock.as_stream()
    #     while True:
    #         data = await s.read(10000)
    #         if not data:
    #             break
    #         res = await self._application.handle(data) 
    #         await s.write(res)
    #     logger.info(' connection closed ')

    #     # while True:
    #     #     data = await sock.recv(self.maxbytes) # <-- would need to set up a buffering mechanism here to handle large requests
    #     #     if not data:
    #     #         break     
    #     #     res = await self._application.handle(data) # req 
    #     #     await sock.sendall(res)  # res
    #     # logger.info(' ----> Close Connection from {}:  (ツ)'.format(addr))


    # async def listen(self, addr='127.0.0.1', port=6666):
    #     logger.info(' ----> ฅ^•ﻌ•^ฅ Serving on {}:{}'.format(addr, port))
        
    #     await curio.tcp_server(addr, port, self.app_handler, backlog=1024, ssl=None, reuse_address=True)
