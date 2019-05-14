import os
import time
import traceback
import dill
import hashlib
import collections
import threading

from grpc_cluster.common import common_type
from grpc_cluster.common import ExceptionWrapper as Ewrapper

from grpc_cluster.proxy import DefaultProxyClient

from grpc_cluster.master import DefaultMasterServer
from grpc_cluster.master import DefaultMasterClient
from grpc_cluster.master import DefaultMasterServicer

from grpc_cluster.worker import DefaultWorkerServer
from grpc_cluster.worker import DefaultWorkerClient

import logging
from grpc_cluster.logger import *


def hash_name(name):
    return hashlib.sha256(name.encode('utf-8')).digest()


class CustomMasterServicer(DefaultMasterServicer):
    def __init__(self):
        DefaultMasterServicer.__init__(self)
        self.welcome_queue = collections.deque()
        
        self.receive_welcome_event = threading.Event()
        

    
    def getWelcomeMessage(self, request, context):
        self.LOG.debug('receive getWelcomeMessage request in CustomMasterServicer')

        token = request.value

        try:
            if len(token) > 0:
                self.welcome_queue.append(token)
        
            self.receive_welcome_event.set()
            
            status = self._getStatusObject('OK')
            
            response = common_type.StatusResponse(result=self.welcome_message, status=status)
            
        except Exception as e:
            status, errcode = self._handleError('getWelcomeMessage', e)
            response = common_type.StatusResponse(status=status, error=errcode)

        return response


class Cluster(object):

    class Master(object):
        def __init__(self, name, port, 
                            max_workers=10,
                            logger_name="Cluster-Master",
                            logger_level='DEBUG'):
            self.name = name
            self.port = port
            self.server = DefaultMasterServer(max_workers=max_workers, 
                                              servicer=CustomMasterServicer)
            
            self.token = hash_name(self.name)
            
            
            def receive_data_callback(request, context):
                token = request.token.value
                tag = request.tag
                data = request.data
                
                #TODO: unpickle data
                unpickle_data = dill.loads(data)
                
                d = {'tag': tag, 'token': token, 'data': unpickle_data}
                
                self._result_queue.append(d)
                self._receive_data_event.set()
                
            
            self.server.servicer.setSendDataCallback(receive_data_callback)
                
            
            self.server.start(port)
            
            self._proxy_client = {}
            self._worker_client = {}
            self._worker_tokens = {}
            
            self._mapping_task = {}
            self._receive_data_event = threading.Event()
            self._result_queue = collections.deque()
            self._worker_num = 0
            
            loadConfig()
            self.LOG = createLoggerFromExistedLogger(logger_name, logger_level)
    
        def _add_proxy_client(self, name, addr):
            class ProxyClientWrapper(object):
                pass
                
            proxy = ProxyClientWrapper()
            proxy.name = name
            proxy.addr = addr
            proxy.client = DefaultProxyClient(addr, name)
            proxy.alive = False
            
            
            self.LOG.debug('add proxy client:')
            self.LOG.debug('    proxy name: {}'.format(proxy.name))
            self.LOG.debug('    proxy addr: {}'.format(proxy.addr))
            self.LOG.debug('    proxy state: {}'.format(proxy.alive))
            
            if name in self._proxy_client:
                raise Exception('proxy name conflict')
                
            self._proxy_client[name] = proxy
            

            if self._proxy_client[name].client.welcome() != None:
                self._proxy_client[name].alive = True
                if self._proxy_client[name].client.login('admin', 'admin') == None:
                    raise Exception('login failed, proxy: {}'.format(name))
            
        def _add_worker_client(self, proxy_name, proxy_ip, name, port):
            class WorkerClientWrapper(object):
                pass
                
            worker = WorkerClientWrapper()
            worker.name = name
            worker.fullname = proxy_name + '/' + name
            worker.addr = proxy_ip + ':' + str(port)
            
            
            worker.token = hash_name(worker.fullname)
            worker.client = DefaultWorkerClient(worker.addr)
            worker.alive = False
            
            self.LOG.debug('add worker client:')
            self.LOG.debug('    worker name: {}'.format(worker.name))
            self.LOG.debug('    worker full: {}'.format(worker.fullname))
            self.LOG.debug('    worker addr: {}'.format(worker.addr))
            self.LOG.debug('    worker token: {}'.format(worker.token))
            self.LOG.debug('    worker state: {}'.format(worker.alive))
            
            
            self._worker_tokens[worker.token] = worker.fullname
            
            if not proxy_name in self._worker_client:
                self._worker_client[proxy_name] = {}
            
            self._worker_num += 1                
            self._worker_client[proxy_name][worker.name] = worker
        
        def _create_venv(self, proxy_name, venv_name, venv_params, venv_requirements):
            if not proxy_name in self._proxy_client:
                return
                
            self.LOG.debug('call _create_venv: proxy_name={} / venv_name={} / venv_params={} / requirements={}'.format(
                                            proxy_name, venv_name, venv_params, venv_requirements))
            
            proxy = self._proxy_client[proxy_name].client
            return proxy.createVenv(venv_name, venv_params, venv_requirements)
        
        def _create_venv_on_all_proxy(self, name, params, requirements):
            for proxy_name in self._proxy_client:
                self._create_venv(proxy_name, name, params, requirements)
        
        def _launch_workers(self, proxy_name, config):
            if not proxy_name in self._proxy_client:
                return
            
            self.LOG.debug('call _launch_workers: proxy_name={}'.format(proxy_name))
            self._proxy_client[proxy_name].client.launchWorkers(config)
            
        def _reset_receive_data_event(self):
            self._receive_data_event.clear()
        
        def _wait_for_receiving_data_event(self):
            self._receive_data_event.wait()
        
        def _reset_receive_welcome_event(self):
            self.server.servicer.receive_welcome_event.clear()
        
        def _wait_for_receiving_welcome_event(self):
        
            self.server.servicer.receive_welcome_event.wait()
        
        def _wait_for_receiving_welcome_messages_from_all_workers(self):
            
            while True:
                self._wait_for_receiving_welcome_event()
                
                while len(self.server.servicer.welcome_queue) > 0:
                    token = self.server.servicer.welcome_queue.popleft()
                    
                    if not token in self._worker_tokens:
                        self.LOG.warning('receive unknown token: {}'.format(token))
                    else:
                        worker_fullname = self._worker_tokens[token]
                    
                        proxy_name, worker_name = worker_fullname.split('/')
                    
                        self._worker_client[proxy_name][worker_name].alive = True
                        self.LOG.debug('receive token from worker: {}'.format(worker_fullname))
                
                self._reset_receive_welcome_event()
                
                if self.check_all_alive():
                    return

        def get_proxy_num(self):
            return len(self._proxy_client)
                    
        def get_worker_num(self):
            return self._worker_num
        
        def get_proxy(self, proxy_name):
            if not proxy_name in self._proxy_client:
                return None
            return self._proxy_client[proxy_name].client
            
        def get_worker(self, proxy_name, worker_name):
            if not proxy_name in self._worker_client:
                return None
            if not worker_name in self._worker_client[proxy_name]:
                return None
            return self._worker_client[proxy_name][worker_name].client
        
        def update_status(self):
        
            self.LOG.debug('call update_status:')
            
            for proxy_name in self._proxy_client:
                if self._proxy_client[proxy_name].client.welcome():
                    self.LOG.debug('    proxy alive: {}'.format(proxy_name))
                    self._proxy_client[proxy_name].alive = True
                
                if proxy_name in self._worker_client:
                    for worker_name in self._worker_client[proxy_name]:
                        worker = self._worker_client[proxy_name][worker_name]
                        if worker.client.welcome():
                            self.LOG.debug('    worker alive: {}'.format(worker_name))
                            worker.alive = True
        
        def check_all_alive(self):
        
            self.LOG.debug('call check_all_alive: ')
            alive = True
            for proxy_name in self._proxy_client:
                #check proxu alive
                alive &= self._proxy_client[proxy_name].alive
                
                if proxy_name in self._worker_client:
                    for worker_name in self._worker_client[proxy_name]:
                        # check worker alive
                        alive &= self._worker_client[proxy_name][worker_name].alive
        
            self.LOG.debug('    result: {}'.format(alive))
        
            return alive
            
            
        def map(self, data_list, tag=''):
        
            self.LOG.debug('call map:')
        
            if len(data_list) == 0:
                raise Exception('in cluster.map, get empty list')
        
            # still mapping
            if len(self._mapping_task) > 0:
                return False
            
            
            data_iter = iter(enumerate(data_list))
            
            
            for proxy_name in self._worker_client:
                for worker_name in self._worker_client[proxy_name]:
                    try:
                        index, data = next(data_iter)
                        worker = self._worker_client[proxy_name][worker_name]
                        
                        #TODO: pickle data
                        pickle_data = dill.dumps(data)
                        
                        
                        self.LOG.debug('    send data to worker: {}'.format(worker.fullname))
                        
                        worker.client.sendData(pickle_data, tag)
                        
                        self._mapping_task[worker.fullname] = index
                    except StopIteration:
                        break

            return True
        
        def reduce(self):
        
            
            mapping_task_num = len(self._mapping_task)
            worker_result_pair = {}
            self.LOG.debug('call reduce: {}'.format(mapping_task_num))
            
            while True:
            
                while len(self._result_queue) > 0:
                    result = self._result_queue.pop()
                    token = result['token']
                    tag = result['tag']
                    data = result['data']
                    
                    self.LOG.debug('    receive data: from token={} / data={}'.format(token, data))
                    
                    if len(token) > 0 and token in self._worker_tokens:
                        worker_name = self._worker_tokens[token]
                        
                        self.LOG.debug('    receive result from worker: {}'.format(worker_name))
                        
                        worker_result_pair[worker_name] = data
                
                if len(worker_result_pair) == mapping_task_num:
                    break
                        
                if len(self._result_queue) == 0:
                    self._wait_for_receiving_data_event()
                    self._reset_receive_data_event()
                
            self.LOG.debug('    ALL DATA RECEIVED')
            
            
            result_list = [None for _ in range(mapping_task_num)]
            
            for worker_name in worker_result_pair:
                index = self._mapping_task[worker_name]
                data = worker_result_pair[worker_name]
                
                result_list[index] = data
            
            self.LOG.debug('        {}'.format(result_list))
            
            self._mapping_task = {}
            
            return result_list
        
        def close(self):
            
            self.LOG.debug('call close: ')
            for proxy_name in self._worker_client:
                for worker_name in self._worker_client[proxy_name]:
                    worker = self._worker_client[proxy_name][worker_name]
                    
                    
                    self.LOG.debug('    send shotdown signal to worker: {}'.format(worker.fullname))
                    worker.client.shutdown()
        
        
    class Worker(object):
        def __init__(self, name, port, 
                            master_name, master_addr, 
                            max_workers=10,
                            logger_name='Cluster-Worker',
                            logger_level='DEBUG'):
                            
            loadConfig()
            self.LOG = createLoggerFromExistedLogger(logger_name, logger_level)
            self.name = name
            self.port = port
            
            self.token = hash_name(name)
            self.LOG.debug('{}'.format(self.token))
            
            self.server = DefaultWorkerServer(max_workers=max_workers)
            self.server.start(self.port)
            
            self.master = DefaultMasterClient(master_addr)
            
            self.task_queue = collections.deque()
            self.receive_task_event = threading.Event()
            
            
            def receive_data_callback(request, context):
                data = request.data
                
                #TODO: unpickle data
                unpickle_data = dill.loads(data)
                
                d = {'tag': request.tag, 'data': unpickle_data}
                self.task_queue.append(d)
                
                self.receive_task_event.set()
                
                
            self.server.servicer.setSendDataCallback(receive_data_callback)
        
        def welcome_to_master(self):
            self.LOG.debug('call welcome_to_master:')
            
            if self.master.welcome(self.token) != None:
                self.LOG.debug('    receive welcome info from master')
        
        def wait_for_task(self):
            self.LOG.debug('call wait_for_task:')
            
            if len(self.task_queue) > 0:
                return
            self.receive_task_event.wait()
            self.receive_task_event.clear()
            
        def get_task(self):
            self.LOG.debug('call get_task:')
        
            d = self.task_queue.pop()
            
            return d['tag'], d['data']
        
        def send_back(self, data):
            self.LOG.debug('call send_back: {}'.format(self.token))
        
            #TODO: pickle data
            pickle_data = dill.dumps(data)
            
            return self.master.sendData(data=pickle_data, tag='', token=self.token)
        
        def set_data_request_callback(self, tag, callback):
            self.server.servicer.setGetMessagesCallback(tag, callback)
        
        def close(self):
            try:
                self.server.stop()
            except:
                pass
        
        def __del__(self):
            self.close()
        
    # Cluster __init__    
    def __init__(self, config_file,
                        logger_name='Cluster',
                        logger_level='DEBUG'):
        
        import yaml
        
        loadConfig()
        self.LOG = createLoggerFromExistedLogger(logger_name, logger_level)
        
        try:
            with open(config_file, 'r') as f:
                config = yaml.safe_load(f)
        except Exception as e:
            self.LOG.error('failed to load configuration file: {}'.format(config_file))
            self.LOG.error(' code: {} / error: {}'.format(type(e).__name__, str(e)))
            return
            
        

        self._config = config
        print(self._config)
        
        self.LOG.debug('creating cluster')
        self._create_cluster()
        
        self.LOG.debug('creating virtual environment')
        self._create_venvs()
        
        self.LOG.debug('launching workers')
        self._launch_workers()
        
        self.LOG.debug('checking servers\' status')
        self._check_all_alive()
        
        ## start cluster
        
    @staticmethod
    def worker(max_workers=10):
        
        worker_path = os.environ['CLUSTER_WORKER_PATH']
        worker_name = os.environ['CLUSTER_WORKER_NAME']
        worker_port = os.environ['CLUSTER_WORKER_PORT']
        master_name = os.environ['CLUSTER_MASTER_NAME']
        master_addr = os.environ['CLUSTER_MASTER_ADDR']
        
        _worker = Cluster.Worker(worker_name, worker_port, master_name, master_addr, max_workers)
        
        _worker.welcome_to_master()
        
        return _worker
        
        
        
    def _create_cluster(self):
        
        self.LOG.debug('call _create_cluster: ')
        
        # master
        self._master_config = self._config.get('master')
        assert self._master_config != None
        
        self._master_name = self._master_config.get('name')
        self._master_addr = self._master_config.get('addr')
        
        assert self._master_name != None
        assert self._master_addr != None
        
        self.LOG.debug('    master name: {}'.format(self._master_name))
        self.LOG.debug('    master addr: {}'.format(self._master_addr))
        
        self._master_ip, _, self._master_port = self._master_addr.rpartition(':')
        
        
        # create master server
        self._master_server = Cluster.Master(self._master_name, self._master_port)
        
        # proxy
        
        self._proxy_config = self._config.get('proxy')
        
        
        for proxy_config in self._proxy_config:
            
            proxy_name = proxy_config.get('name')
            proxy_addr = proxy_config.get('addr')
            assert proxy_name != None
            assert proxy_addr != None
            
            self.LOG.debug('    proxy name: {}'.format(proxy_name))
            self.LOG.debug('    proxy addr: {}'.format(proxy_addr))
            
            proxy_ip, _, _ = proxy_addr.rpartition(':')
            
            
            # create proxy client
            self._master_server._add_proxy_client(proxy_name, proxy_addr)
            
            
            
            if proxy_config.get('workers') != None:
                for worker_config in proxy_config.get('workers'):
                
                    worker_name = worker_config.get('name')
                    worker_port = worker_config.get('port')
                    assert worker_name != None
                    assert worker_port != None
                    
                    self.LOG.debug('    worker name: {}'.format(worker_name))
                    self.LOG.debug('    worker port: {}'.format(worker_port))
                    
                    assert worker_config.get('entrypoint') != None or proxy_config.get('default_entrypoint') != None
                    assert worker_config.get('venv') != None or proxy_config.get('default_venv') != None
                    
                    # create worker client
                    self._master_server._add_worker_client(proxy_name, proxy_ip, worker_name, worker_port)
                    
    
    def _create_venvs(self):
    
        self.LOG.debug('call _create_venvs: ')
        
        self._venv_config = self._config.get('venv')
        
        if self._venv_config == None:
            return
        
        for venv in self._venv_config:
            venv_name = venv.get('name')
            venv_params = venv.get('params')
            venv_requirements = venv.get('requirements')
            assert venv_name != None
            #assert venv_requirements != None
            
            self.LOG.debug('    venv name: {}'.format(venv_name))
            self.LOG.debug('    venv params: {}'.format(venv_params))
            self.LOG.debug('    venv requ: {}'.format(venv_requirements))
            
            self._master_server._create_venv_on_all_proxy(venv_name, venv_params, venv_requirements)
    
    def _launch_workers(self):
    
        self.LOG.debug('call _launch_workers: ')
    
        for proxy_config in self._proxy_config:
            name = proxy_config.get('name')
            configure = {
                'master': self._master_config,
                'proxy': proxy_config
            }
            
            self.LOG.debug('    proxy name: {}'.format(name))
            self.LOG.debug('    proxy conf: {}'.format(configure))
            
            self._master_server._launch_workers(name, configure)
            
        self._master_server._wait_for_receiving_welcome_messages_from_all_workers()
        
    def _check_all_alive(self):
    
        self.LOG.debug('call _check_all_alive: ')
    
        self._master_server.update_status()
        
        return self._master_server.check_all_alive()
  
    def get_proxy_num(self):
        return self._master_server.get_proxy_num()    


    def get_worker_num(self):
        return self._master_server.get_worker_num()

    def map(self, data_list):

        self.LOG.debug('call map: ')    
    
        return self._master_server.map(data_list)
    
    def reduce(self):
    
        self.LOG.debug('call reduce: ')
    
        result_list = self._master_server.reduce()
        return result_list
    
    def close(self):
        try:
            self.LOG.debug('call close: ')
    
            self._master_server.close()
            
        except:
            pass
        
    def __del__(self):
        self.close()
        
        
        
        
        
        
