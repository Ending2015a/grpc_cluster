import random
import time
from grpc_cluster import Cluster

config_path = 'cluster_config.yml'

cluster = Cluster(config_path)


# call cluster.get_worker_num() to obtain the number of workers you had created
print('worker_num: {}'.format(cluster.get_worker_num()))

# call cluster.get_proxy_num() to obtain the number of proxy servers
print('proxy_num: {}'.format(cluster.get_proxy_num()))





# --- some utilities
def generate_sample(num):
    return random.sample(range(1 ,100), num)
    
def generate_samples(num, sample):
    return [generate_sample(sample) for i in range(num)]
# ---  


# run 100 samples
for i in range(100):

    # get samples
    actions = generate_samples(cluster.get_worker_num() * 2, 5)
    print('generate actions: {}'.format(actions))


    # map actions to workers and set timeout = 10 (sec)
    print('map actions to workers')
    assert cluster.map(actions, timeout=13)



    print('wait for results')

    # waiting and collecting results from workers
    done, results = cluster.reduce(block=True)
    
    if None in results:
        print('Oops! some tasks failed')
    
    print('{}: {}'.format(done, results))

cluster.close()
