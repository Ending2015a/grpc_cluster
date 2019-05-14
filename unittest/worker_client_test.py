from grpc_cluster.worker import DefaultWorkerClient

client = DefaultWorkerClient('localhost:50051')

data = '大家好'.encode('utf-8')

print(len(data))

assert client.sendData(tag='Joe', data=data)

assert client.sendMessage(tag='joe', message=data.decode('utf-8'))

res = client.getMessages(['one', 'three', 'hello'])

assert res != None
print(res)

assert client.shutdown()
