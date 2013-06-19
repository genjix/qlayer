import fullnode
import service
import publisher
import qtable
import sys
import time

def main():
    config = {
        "service-port": 9091,
        "block-publish-port": 9092,
        "tx-publish-port": 9093,
        "database": "/home/genjix/database",
        "stop-secret": "surfing2",
        "dbhost": "localhost",
        "dbname": "bccache",
        "dbuser": "genjix",
        "dbpassword": "surfing2"
    }
    # Load config here.
    node = fullnode.FullNode()
    print "Starting node..."
    if not node.start(config):
        return 1
    print "Node started."
    print "Starting publisher..."
    publish = publisher.Publisher(node)
    if not publish.start(config):
        return 1
    print "Publisher started."
    print "Starting QTable..."
    table = qtable.QueryCacheTable(node)
    if not table.start(config):
        return 1
    print "QTable started."
    print 'Starting the server...'
    serv = service.ServiceServer()
    serv.start(config, node)
    while not serv.stopped:
        table.update()
        time.sleep(1)
    print "Server stopped."
    print "Stopping node..."
    if not node.stop():
        return 1
    print "Node stopped."
    return 0

if __name__ == "__main__":
    sys.exit(main())

