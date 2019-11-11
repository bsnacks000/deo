import asyncio  

from aiorpc.application import TCPServer

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    server = TCPServer()
    server.run_server(loop)