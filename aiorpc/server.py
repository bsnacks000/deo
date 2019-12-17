#import curio  
import asyncio 
import signal
import logging 
import coloredlogs
import functools

logger = logging.getLogger('asyncio')
coloredlogs.install(level=logging.INFO)

class TCPServer(object):
    """ Recieve up to max bytes via TCP and pass to application for processing.
    """

    def __init__(self, application, maxbytes=100000):
        self._application = application
        self.maxbytes = maxbytes

    @staticmethod
    def _shutdown(loop):
        logging.warn('Cancelling tasks...')
        for task in asyncio.Task.all_tasks():
            task.cancel()


    def listen(self, addr='127.0.0.1', port=6666):
        
        async def _app_handler(reader, writer):
            addr = writer.get_extra_info('peername')
            logger.info(' <----- ( •_•) Connection from {}'.format(addr))
            data = await reader.read(self.maxbytes)

            res = await self._application.handle(data)
            
            writer.write(res)
            await writer.drain()
            
            logger.info(' ----> Closing Connection from {}:  (ツ)'.format(addr))
            writer.close()

        #XXX note this is not particularly great, but ok while still in development
        # should break out into seperate method. Also get direct control of loop.
        
        try:
            loop = asyncio.get_event_loop()
            loop.add_signal_handler(signal.SIGHUP, functools.partial(self._shutdown, loop))
            loop.add_signal_handler(signal.SIGTERM, functools.partial(self._shutdown, loop))


            coro = asyncio.start_server(_app_handler,addr, port, loop=loop)
            server = loop.run_until_complete(coro)
            
            logger.info(' ----> ฅ^•ﻌ•^ฅ Serving on {}:{}'.format(addr, port))
            loop.run_forever()
        
        except KeyboardInterrupt:
            pass

        except asyncio.CancelledError as err:
            logging.warn(str(err))

        finally:
            logger.warn('Cleaning up')
            
            self._application.threadpool_executor.shutdown(wait=True)
            self._application.processpool_executor.shutdown(wait=True)
            
            server.close()
            loop.run_until_complete(server.wait_closed())
            loop.close()