from grpc_cluster.proxy import DefaultProxyServer
import sys

server = DefaultProxyServer()

if len(sys.argv) > 1:
    server.start(int(sys.argv[1]))
else:
    server.start(50040)
