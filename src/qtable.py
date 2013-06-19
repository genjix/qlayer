import bitcoin
import psycopg2
import sys
from decimal import Decimal as D

class QueryCacheTable:

    def __init__(self, node):
        self._chain = node.blockchain
        self._node = node
        self._latest_blk_hash = None

    def start(self, config):
        self._dbconn = psycopg2.connect(
            "host='%s' dbname='%s' user='%s' password='%s'" %
                (config["dbhost"], config["dbname"],
                 config["dbuser"], config["dbpassword"]))
        self._cursor = self._dbconn.cursor()
        def examine_blocks(ec, fork_point, added, removed):
            if ec:
                return
            for i, blk in enumerate(added):
                depth = fork_point + 1 + i
                self.examine_blk(blk)
            self._node.blockchain.subscribe_reorganize(examine_blocks)
        self._node.blockchain.subscribe_reorganize(examine_blocks)
        def examine_unconfirm_tx(ec, tx, missing):
            if ec:
                return
            self.examine_tx(tx)
            self._node.subscribe_transaction(examine_unconfirm_tx)
        self._node.subscribe_transaction(examine_unconfirm_tx)
        return True

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
        self._latest_block_hash = bitcoin.hash_block_header(blk)
        for tx in blk.transactions:
            self._check(tx, True)

    def examine_tx(self, tx):
        self._check(tx, False)

    def _check(self, tx, confirmed):
        tx_hash = bitcoin.hash_transaction(tx)
        for i, input in enumerate(tx.inputs):
            prevout = input.previous_output
            # If output exists, this will set the spend for it
            self._cursor.execute("""
                UPDATE history
                SET
                    spend_point_hash=decode(%s, 'hex'),
                    spend_point_index=%s,
                    confirmed_debit=%s
                WHERE
                    output_point_hash=decode(%s, 'hex') AND
                    output_point_index=%s
                """, (tx_hash, i, prevout.hash, prevout.index, confirmed))
        for i, output in enumerate(tx.outputs):
            payaddr = bitcoin.payment_address()
            if not bitcoin.extract(payaddr, output.output_script):
                continue
            # If payment exists, then this will update it to be confirmed.
            if confirmed:
                self._cursor.execute("""
                    UPDATE history
                    SET confirmed_credit=true
                    WHERE
                        output_point_hash=%s AND
                        output_point_index=%s
                """, (tx_hash, i))
            # If payment doesn't exist, this will insert a new row.
            self._cursor.execute("""
                INSERT INTO history (
                    address,
                    output_point_hash,
                    output_point_index,
                    value,
                    confirmed_credit
                ) SELECT
                    %s, decode(%s, 'hex'), %s, %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM history
                    WHERE
                        output_point_hash=%s AND
                        output_point_index=%s
                )
            """, (addr, tx_hash, i, output.value, confirmed, tx_hash, i))
        self._dbconn.commit()

