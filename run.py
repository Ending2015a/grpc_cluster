from grpc_cluster import Cluster

worker = Cluster.worker()

try:
    while True:
        print('waiting for task')
        worker.wait_for_task()
        
        tag, values = worker.get_task()  #return tag & action
        print('receive task: {}'.format(values))
        
        
        result = sum(values)
        
        print('send back: {}'.format(result))
        worker.send_back(result)
        

except KeyboardInterrupt:
    print('Shutdown')

worker.close()
