import time
import random
from grpc_cluster import Cluster

worker = Cluster.worker()

try:
    while True:
        print('waiting for task')
        worker.wait_for_task()
        
        ID, tag, values = worker.get_task()  #return tag & action
        print('receive task: ID={} / tag={} / values={}'.format(ID, tag, values))

        t = random.randint(3, 15)
        print('{} sec works:'.format(t))

        for i in range(t):
            print('    remain {} sec'.format(t-i))
            time.sleep(1)
        print('done')
        
        result = sum(values)
        
        print('submit: ID={} / tag={} / result={}'.format(ID, tag, result))
        worker.submit(data=result, ID=ID, tag=tag)
        

except KeyboardInterrupt:
    print('Shutdown')

worker.close()
