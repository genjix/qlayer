import bitcoin
from future import Future

class SyncBlockchain:

    def __init__(self, chain):
        self._chain = chain

    def fetch_block_header(self, index):
        future = Future()
        self._chain.py_fetch_block_header(index, future)
        return future.get()

