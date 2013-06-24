import bitcoin
import sys
import threading
from future import Future

class Subscriber:

    def __init__(self):
        self.handlers = []
        self.lock = threading.Lock()

    def subscribe(self, handler):
        with self.lock:
            self.handlers.append(handler)

    def relay(self, *args):
        with self.lock:
            notify_copy = self.handlers[:]
            self.handlers = []
        [handle(*args) for handle in self.handlers]

class FullNode:

    def __init__(self):
        self._net_pool = bitcoin.threadpool(1)
        self._disk_pool = bitcoin.threadpool(1)
        self._mem_pool = bitcoin.threadpool(1)
        self._hosts = bitcoin.hosts(self._net_pool)
        self._handshake = bitcoin.handshake(self._net_pool)
        self._network = bitcoin.network(self._net_pool)
        self._protocol = bitcoin.protocol(self._net_pool,
                                          self._hosts,
                                          self._handshake,
                                          self._network)
        self._chain = bitcoin.leveldb_blockchain(self._disk_pool)
        self._poller = bitcoin.poller(self._mem_pool, self._chain)
        self._txpool = bitcoin.transaction_pool(self._mem_pool, self._chain)
        pars = bitcoin.create_session_params(self._handshake,
				                             self._protocol,
				                             self._chain,
				                             self._poller,
				                             self._txpool)
        self._session = bitcoin.session(self._net_pool, pars)
        self._tx_subscribe = Subscriber()

    def subscribe_transaction(self, handler):
        self._tx_subscribe.subscribe(handler)

    def start(self, config):
        self._protocol.subscribe_channel(self._new_channel)
        # Blockchain
        future = Future()
        self._chain.start(config["database"], future)
        ec, = future.get()
        if ec:
            print >> sys.stderr, "Couldn't start blockchain:", str(ec)
            return False
        # Transaction pool
        self._txpool.start()
        # Session
        future = Future()
        self._session.start(future)
        ec, = future.get()
        if ec:
            print >> sys.stderr, "Couldn't start session:", str(ec)
            return False
        return True

    def stop(self):
        # Ignore callback from session.stop()
        self._session.stop(lambda ec: None)
        self._net_pool.stop()
        self._disk_pool.stop()
        self._mem_pool.stop()
        self._net_pool.join()
        self._disk_pool.join()
        self._mem_pool.join()
        self._chain.stop()
        return True

    @property
    def blockchain(self):
        return self._chain
    
    @property
    def transaction_pool(self):
        return self._txpool

    @property
    def protocol(self):
        return self._protocol

    def _new_channel(self, ec, node):
        if ec:
            print >> sys.stderr, "Error with new channel:", str(ec)
            return
        node.subscribe_transaction(lambda ec, tx: self._recv_tx(ec, tx, node))
        self._protocol.subscribe_channel(self._new_channel)

    def _recv_tx(self, ec, tx, node):
        if ec:
            print >> sys.stderr, "Error receiving tx:", str(ec)
            return
        def handle_confirm(ec):
            if ec:
                print >> sys.stderr, "Error confirming tx:", str(ec)
        self._txpool.store(tx, handle_confirm,
            lambda ec, missing: self._tx_validated(ec, missing, tx))
        node.subscribe_transaction(lambda ec, tx: self._recv_tx(ec, tx, node))

    def _tx_validated(self, ec, missing_inputs, tx):
        if ec:
            print >> sys.stderr, "Error validating tx:", str(ec)
            return
        tx_hash = bitcoin.hash_transaction(tx).encode("hex")
        print "Accepted transaction:", tx_hash
        # missing_inputs are the inputs for this transaction which
        # depend on an output from another unconfirmed transaction
        # which we have in the memory pool (and have validated ourselves).
        self._tx_subscribe.relay(ec, tx, missing_inputs)

