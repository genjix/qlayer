import bitcoin
import psycopg2
from decimal import Decimal as D

class QueryCacheTable:

    def __init__(self, chain):
        self._dbconn = psycopg2.connect("host='localhost' dbname='bccache' user='genjix' password='surfing2'")
        self._cursor = self._dbconn.cursor()
        self._chain = chain

    def update(self):
        self._cursor.execute("""
            SELECT address FROM queue WHERE last_update_time IS NULL
        """)
        results = self._cursor.fetchall()
        for addr, in results:
            payaddr = bitcoin.payment_address()
            if not payaddr.set_encoded(addr):
                print >> sys.stderr, "Ignoring invalid Bitcoin address:", addr
                continue
            bitcoin.fetch_history(self._chain, payaddr,
                lambda ec, inpoints, outpoints: \
                    self._history_fetched(ec, inpoints, outpoints, addr))
            print addr
    
    def _history_fetched(self, ec, inpoints, outpoints, addr):
        if ec:
            print >> sys.stderr, "History failed for ", addr
            return
        bitcoin.fetch_output_values(self._chain, outpoints,
            lambda ec, values: \
                self._values_fetched(ec, values, inpoints, outpoints, addr))

    def _values_fetched(self, ec, values, inpoints, outpoints, addr):
        if ec:
            print >> sys.stderr, "Values failed for ", addr
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
        self._dbconn.commit()

    def examine_blk(self, blk):
        pass

    def examine_tx(self, tx):
        pass

def blockchain_started(ec, table):
    if ec:
        print >> sys.stderr, str(ec)
        return
    table.update()

def main():
    pool = bitcoin.threadpool(1)
    chain = bitcoin.leveldb_blockchain(pool)
    table = QueryCacheTable(chain)
    chain.start("database",
        lambda ec: blockchain_started(ec, table))
    raw_input()
    pool.stop()
    pool.join()
    chain.stop()

if __name__ == "__main__":
    main()

