import zmq

class Publisher:

    def __init__(self, node):
        self._context = zmq.Context(1)
        self._socket_block = self._context.socket(zmq.PUB)
        self._socket_tx = self._context.socket(zmq.PUB)
        self._node = node

    def start(self, config):
        bind_addr = "tcp://*:"
        self._socket_block.bind("tcp://*:%s" % config["block-publish-port"])
        self._socket_tx.bind("tcp://*:%s" % config["tx-publish-port"])
        def publish_blk(ec, fork_point, added, removed):
            if ec:
                return
            for i, blk in enumerate(added):
                depth = fork_point + 1 + i
                self.send_blk(depth, blk)
            self._node.blockchain.subscribe_reorganize(publish_blk)
        self._node.blockchain.subscribe_reorganize(publish_blk)
        def publish_tx(ec, tx, missing):
            if ec:
                return
            self.send_tx(tx)
            self._node.subscribe_transaction(publish_tx)
        self._node.subscribe_transaction(publish_tx)
        return True

    def stop(self):
        self._socket_block.close()
        self._socket_tx.close()
        self._context.term()
        return True

    def send_blk(self, depth, blk):
        # We could use send_json or send_pyobj instead.
        self._socket_block.send_multipart(["depth", "rawblock"])

    def send_tx(self, tx):
        self._socket_tx.send("rawtx")

