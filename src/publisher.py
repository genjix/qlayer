import zmq

class Publisher:

    def __init__(self):
        self._context = zmq.Context(1)
        self._socket_block = self._context.socket(zmq.PUB)
        self._socket_tx = self._context.socket(zmq.PUB)

    def start(self, config):
        bind_addr = "tcp://*:"
        self._socket_block.bind("tcp://*:%s" % config["block-publish-port"])
        self._socket_tx.bind("tcp://*:%s" % config["tx-publish-port"])

    def stop(self):
        self._socket_block.close()
        self._socket_tx.close()
        self._context.term()

    def send_blk(self, depth, blk):
        # We could use send_json or send_pyobj instead.
        self._socket_block.send_multipart([depth, "rawblock"])

    def send_tx(self, tx):
        self._socket_tx.send("rawtx")

