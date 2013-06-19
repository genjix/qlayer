import bitcoin
import psycopg2
import sys
from decimal import Decimal as D

class QueryCacheTable:

    def __init__(self, chain):
        self._dbconn = psycopg2.connect("host='localhost' dbname='bccache' user='genjix' password='surfing2'")
        self._cursor = self._dbconn.cursor()
        self._chain = chain
        self._latest_blk_hash = None

    def update(self):
        self._cursor.execute("""
            SELECT address FROM queue WHERE last_update_time IS NULL
        """)
        results = self._cursor.fetchall()
        for addr, in results:
            self._process(addr)

    def _process(self, addr):
        payaddr = bitcoin.payment_address()
        if not payaddr.set_encoded(addr):
            print >> sys.stderr, "Ignoring invalid Bitcoin address:", addr
            print >> sys.stderr, "Reason:", str(ec)
            return
        bitcoin.fetch_history(self._chain, payaddr,
            lambda ec, outpoints, inpoints: \
                self._history_fetched(ec, outpoints, inpoints,
                                      addr, self._latest_blk_hash))
    
    def _history_fetched(self, ec, outpoints, inpoints, addr, blk_hash):
        if ec:
            print >> sys.stderr, "History failed for", addr
            print >> sys.stderr, "Reason:", str(ec)
            return
        bitcoin.fetch_output_values(self._chain, outpoints,
            lambda ec, values: \
                self._values_fetched(ec, values, outpoints, inpoints,
                                     addr, blk_hash))

    def _values_fetched(self, ec, values, outpoints, inpoints, addr, blk_hash):
        if ec:
            print >> sys.stderr, "Values failed for", addr
            print >> sys.stderr, "Reason:", str(ec)
            return
        for outpoint, value, inpoint in zip(outpoints, values, inpoints):
            value = D(value) / 10**8
            if inpoint.is_null():
                spend_point_hash = None
                spend_point_index = None
                confirmed_debit = False
            else:
                spend_point_hash = inpoint.hash.encode("hex")
                spend_point_index = inpoint.index
                confirmed_debit = True
            self._cursor.execute("""
                INSERT INTO history (
                    address,
                    output_point_hash,
                    output_point_index,
                    value,
                    confirmed_credit,
                    spend_point_hash,
                    spend_point_index,
                    confirmed_debit
                ) VALUES (
                    %s, decode(%s, 'hex'), %s,
                    %s, %s,
                    decode(%s, 'hex'), %s, %s)
            """, (addr, outpoint.hash.encode("hex"), outpoint.index, value,
                  True, spend_point_hash, spend_point_index, confirmed_debit))
            self._cursor.execute("""
                UPDATE queue
                SET last_update_time=now()
                WHERE address=%s
            """, (addr,))
            print addr, outpoint, value, inpoint
        if blk_hash != self._latest_blk_hash:
            print >> sys.stderr, "Retrying", addr
            self._dbconn.rollback()
            self._process(addr)
            return
        self._dbconn.commit()

    def examine_blk(self, blk):
        pass

    def examine_tx(self, tx):
        pass

def blockchain_started(ec):
    if ec:
        print >> sys.stderr, str(ec)
        return

def main():
    import time
    pool = bitcoin.threadpool(1)
    chain = bitcoin.leveldb_blockchain(pool)
    table = QueryCacheTable(chain)
    chain.start("database", blockchain_started)
    try:
        while True:
            table.update()
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    pool.stop()
    pool.join()
    chain.stop()

if __name__ == "__main__":
    main()

