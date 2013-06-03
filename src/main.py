import fullnode
import service
import sys

def main():
    config = {
        "service-port": 8777,
        "block-publish-port": 8778,
        "tx-publish-port": 8779,
        "database": "/home/stable/database",
        "stop-secret": "surfing2"
    }
    # Load config here.
    node = fullnode.FullNode()
    print "Starting node..."
    if not node.start(config):
        return 1
    print "Node started."
    service.start_thrift_server(config, node)
    print "Server stopped."
    print "Stopping node..."
    if not node.stop():
        return 1
    print "Node stopped."
    return 0

if __name__ == "__main__":
    sys.exit(main())

