import curio  
import logging 

logger = logging.getLogger(__name__)


class TCPServer(object):
    """ Recieve up to max bytes via TCP and pass to application for processing.
    """

    def __init__(self, application, maxbytes=100000):
        self._application = application
        self.maxbytes = maxbytes


    async def app_handler(self, sock, addr):
        logger.info(' <----- ( •_•) Connection from {}'.format(sock.getpeername()))
        while True:
            data = await sock.recv(self.maxbytes) # <-- would need to set up a buffering mechanism here to handle large requests
            if not data:
                break     
            res = await self._application.handle(data) # req 
            await sock.sendall(res)  # res
        logger.info(' ----> Close Connection from {}:  (ツ)'.format(sock.getpeername()))

    async def listen(self, addr='127.0.0.1', port=6666):
        logger.info(' ----> ฅ^•ﻌ•^ฅ Serving on {}:{}'.format(addr, port))
        
        await curio.tcp_server(addr, port, self.app_handler, backlog=1024, ssl=None, reuse_address=True)
