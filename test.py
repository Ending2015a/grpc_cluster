import random

from grpc_cluster import Cluster

config_path = 'cluster_config.yml'

cluster = Cluster(config_path)

def generate_sample(num):
    return random.sample(range(1 ,100), num)
    
def generate_samples(num, sample):
    return [generate_sample(sample) for i in range(num)]
        


for i in range(5):
    actions = generate_samples(2, 5)
    print('generate actions: {}'.format(actions))

    print('map actions to workers')
    assert cluster.map(actions)

    print('wait for results')
    results = cluster.reduce() #return list
    print(results)

cluster.close()
