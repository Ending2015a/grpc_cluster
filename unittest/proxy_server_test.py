from grpc_cluster.proxy import ProxyServer


server = ProxyServer()

server.start(50051)
