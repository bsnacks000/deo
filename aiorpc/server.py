#import curio  
import asyncio 
import aiologger


class TCPServer(object):
    """ Recieve up to max bytes via TCP and pass to application for processing.
    """

    def __init__(self, application, maxbytes=100000):
        self._application = application
        self.maxbytes = maxbytes

    async def listen(self, addr='127.0.0.1', port=6666):
        logger = aiologger.Logger.with_default_handlers(name=__name__)

        async def _app_handler(reader, writer):
            addr = writer.get_extra_info('peername')
            logger.info(' <----- ( •_•) Connection from {}'.format(addr))
            data = await reader.read(self.maxbytes)

            res = await self._application.handle(data)
            
            writer.write(res)
            await writer.drain()
            
            logger.info(' ----> Closing Connection from {}:  (ツ)'.format(addr))
            writer.close()

        try:
            server = await asyncio.start_server(_app_handler, addr, port)
            async with server:
                logger.info(' ----> ฅ^•ﻌ•^ฅ Serving on {}:{}'.format(addr, port))
                await server.serve_forever()
        except KeyboardInterrupt:
            pass 
        finally:
            self._application.threadpool_executor.shutdown(wait=True)
            self._application.threadpool_executor.shutdown(wait=True)
            await server.close()
            await logger.shutdown()