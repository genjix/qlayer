import threading
import time
import sync_blockchain
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol
from thrift.server import TServer
from interface import QueryService

class ServerExit(Exception):
    pass

class QueryHandler:

    def __init__(self, config, node):
        self._stop_secret = config["stop-secret"]
        self._chain = sync_blockchain.SyncBlockchain(node.blockchain)
        self._txpool = node.transaction_pool
        self._protocol = node.protocol

    def initialize(self, stop_event):
        self._stop_event = stop_event

    def stop(self, secret):
        if secret != self._stop_secret:
            return False
        print "Stopping server..."
        self._stop_event.set()
        return True

    def block_header_by_depth(self, depth):
        # fetch the block header from the blockchain
        # convert it to the Apache Thrift format
        # return it.
        pass

    def block_header_by_hash(self, hash):
        pass

    def block_transaction_hashes_by_depth(self, depth):
        pass

    def block_transaction_hashes_by_hash(self, hash):
        pass

    def block_depth(self, hash):
        pass

    def last_depth(self):
        pass

    def transaction(self, hash):
        pass

    def transaction_index(self, hash):
        pass

    def spend(self, outpoint):
        pass

    def outputs(self, address):
        pass

    def transaction_pool_transaction(self, hash):
        pass

    def broadcast_transaction(self, data):
        pass

class Servant(threading.Thread):

    daemon = True

    def __init__(self, server):
        self._server = server
        self.stop_event = threading.Event()
        super(Servant, self).__init__()

    def run(self):
        self._server.serve()

def start_thrift_server(config, node):
    handler = QueryHandler(config, node)
    processor = QueryService.Processor(handler)
    transport = TSocket.TServerSocket(port=config["service-port"])
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)

    # You could do one of these for a multithreaded server
    #server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    #server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)

    serv = Servant(server)
    handler.initialize(serv.stop_event)
    print 'Starting the server...'
    serv.start()
    while not serv.stop_event.isSet():
        time.sleep(1)

if __name__ == "__main__":
    config = {
        "service-port": 8777
    }
    start_thrift_server(config, None)

