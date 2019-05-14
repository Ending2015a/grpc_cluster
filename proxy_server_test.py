from grpc_cluster.proxy import DefaultProxyServer


server = DefaultProxyServer()

server.start(50051)
