import time
import random
from grpc_cluster import Cluster

worker = Cluster.worker()

try:
    while True:
        print('waiting for task')
        worker.wait_for_task()
        
        tag, values = worker.get_task()  #return tag & action
        print('receive task: {}'.format(values))

        t = random.randint(3, 8)
        print('{} sec works:'.format(t))

        for i in range(t):
            print('    remain {} sec'.format(t-i))
            time.sleep(i)
        print('done')
        
        result = sum(values)
        
        print('send back: {}'.format(result))
        worker.send_back(result)
        

except KeyboardInterrupt:
    print('Shutdown')

worker.close()
