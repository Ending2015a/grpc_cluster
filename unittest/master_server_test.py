from grpc_cluster.master import DefaultMasterServer



def getData(request, context):
    tag = request.tag
    data = request.data
    
    print('get tag={}, data={}'.format(tag, data.decode('utf-8')))
    
def getMessage(request, context):
    tag = request.tag
    msg = request.msg
    
    print('get tag={}, msg={}'.format(tag, msg))
    

server = DefaultMasterServer()

server.servicer.setSendDataCallback(getData)

server.servicer.setSendMessageCallback(getMessage)

server.start(50051, block=True)
