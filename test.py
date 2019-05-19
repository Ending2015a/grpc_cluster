import random
import time
from grpc_cluster import Cluster

config_path = 'cluster_config.yml'

cluster = Cluster(config_path)


print('worker_num: {}'.format(cluster.get_worker_num()))
print('proxy_num: {}'.format(cluster.get_proxy_num()))

def generate_sample(num):
    return random.sample(range(1 ,100), num)
    
def generate_samples(num, sample):
    return [generate_sample(sample) for i in range(num)]
        


for i in range(5):
    actions = generate_samples(cluster.get_worker_num(), 5)
    print('generate actions: {}'.format(actions))

    print('map actions to workers')
    assert cluster.map(actions)

    print('wait for results')
    
    done = False
    while not done:
        print('workers are still working...')
        time.sleep(1)
        done, results = cluster.reduce(block=False)
        
    print(results)

cluster.close()
