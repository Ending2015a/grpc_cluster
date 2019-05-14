from grpc_cluster.master import DefaultMasterClient

client = DefaultMasterClient('localhost:50051')

data = '大家好'.encode('utf-8')

print(len(data))

assert client.sendData(data, tag='Joe')

message = data.decode('utf-8')

assert client.sendMessage(message, tag='joe')


