import bitcoin
import psycopg2
import sys

def main(bcaddr):
    address = bitcoin.payment_address()
    if not address.set_encoded(bcaddr):
        print >> sys.stderr, "Invalid Bitcoin address"
        return -1
    dbconn = psycopg2.connect("host='localhost' dbname='bccache' user='genjix' password='surfing2'")
    cursor = dbconn.cursor()
    cursor.execute("INSERT INTO queue (address) VALUES (%s)", (bcaddr,))
    dbconn.commit()
    return 0

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print "Usage: loadaddr ADDRESS"
    else:
        sys.exit(main(sys.argv[1]))

