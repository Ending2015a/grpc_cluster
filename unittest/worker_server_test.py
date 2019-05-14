from grpc_cluster.worker import DefaultWorkerServer

def getOne(request, context):
    return 1

def getTwo(request, context):
    return 2

def getThree(request, context):
    return 3

def getHello(request, context):
    return 'Hello'

def sendData(request, context):
    tag = request.tag
    data = request.data

    print('get tag={}, data={}'.format(tag, data.decode('utf-8')))

def sendMessage(request, context):
    tag = request.tag
    msg = request.msg

    print('get tag={}, msg={}'.format(tag, msg))

server = DefaultWorkerServer()

server.servicer.setSendDataCallback(sendData)
server.servicer.setSendMessageCallback(sendMessage)
server.servicer.setGetMessagesCallback('one', getOne)
server.servicer.setGetMessagesCallback('two', getTwo)
server.servicer.setGetMessagesCallback('three', getThree)
server.servicer.setGetMessagesCallback('hello', getHello)

server.start(50051, block=True)


