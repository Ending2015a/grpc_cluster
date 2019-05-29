import re
import os
import copy
import time
import traceback
import dill
import hashlib
import collections
import threading
import datetime

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

worker_format = '{}@{}:{}'
worker_pattern = r'([\w.]+)@([\w.]+):(\d+)$'
worker_parser = re.compile(worker_pattern)

task_pattern = r'(.*):(\d+):(.*)$'
task_parser = re.compile(task_pattern)

def hash_name(name):
    return hashlib.sha256(name.encode('utf-8')).digest()
    

def parse_worker_fullname(fullname):
    match = worker_parser.match(fullname)
    if match == None:
        return [None for _ in range(worker_parser.groups)]
    return [match.group(i+1) for i in range(worker_parser.groups)]

def create_worker_fullname(proxy_name, worker_name, worker_port):
    fullname = worker_format.format(worker_name, proxy_name, worker_port)
    return fullname

def parse_task_tag(tag):
    match = task_parser.match(tag)
    if match == None:
        return [None for _ in range(task_parser.groups)]
    return [match.group(i+1) for i in range(task_parser.groups)]

def get_current_time_plus_delta(seconds):
    _time = datetime.datetime.now() + datetime.timedelta(seconds=seconds)
    return _time.time()

class Cluster(object):

    # master
    class Master(object):
        def __init__(self, name, port, master_config,
                           max_workers=5,
                           logger_name="Cluster-Master",
                           logger_level='DEBUG'):
            self.name = name
            self.port = port
            self.master_config = master_config
            self.server = DefaultMasterServer(max_workers=max_workers)
            
            self.token = hash_name(self.name)
            
            loadConfig()
            self.LOG = createLoggerFromExistedLogger(logger_name, logger_level)
            
            class MessageHelper:
            
                def __init__(self, logger):
                
                    # logger
                    self.LOG = logger
                
                    # event flags
                    self._receive_data_event = threading.Event()
                    self._receive_welcome_request_event = threading.Event()
                    
                    # queue
                    self._task_queue = collections.deque()
                    self._data_queue = collections.deque()
                    
                    self._welcome_request_queue = collections.deque()
                    
                    # initialize flags
                    self._receive_data_event.clear()
                    self._receive_welcome_request_event.clear()
                    
                    # the list of tasks that have been mapped to the workers and still have not been sent back yet.
                    self._mapping_task = {}
                    
                    self._task_buffer = {}
                    
                    
                    # index to identify task
                    self._task_index = 0
                    
                # callback
                def receive_data_callback(self, request, context):
                    token = request.token.value
                    tag = request.tag
                    data = request.data
                    
                    unpickle_data = dill.loads(data)
                    
                    _type, _ID, _tag = parse_task_tag(tag)
                    
                    d = {'id': _ID, 'tag': _tag, 'token': token, 'data': unpickle_data}
                    self.LOG.debug('call receive_data_callback: {}'.format(d))
                    
                    if _type == 'task':
                        self._task_queue.append(d)
                        self._receive_data_event.set()
                    else:
                        self._data_queue.append(d)
                        self._receive_data_event.set()
                    
                    
                    
                # callback
                def receive_welcome_message_callback(self, request, context):
                    token = request.value
                    
                    self.LOG.debug('    receive welcome request, token: {}'.format(token))
                    
                    self._welcome_request_queue.append(token)
                    self._receive_welcome_request_event.set()
                    
                def getData(self, wait=False):
                    while len(self._data_queue) == 0:
                        if wait:
                            self._receive_data_event.wait()
                            self._receive_data_event.clear()
                        else:
                            return None
                    return self._data_queue.popleft()
                
                def getTask(self, wait=False, timeout=None):
                    if wait:
                        self._receive_data_event.wait(timeout)
                        self._receive_data_event.clear()
                    
                    if len(self._task_queue) == 0:
                        return None
                    return self._task_queue.popleft()
                
                def getWelcomeRequest(self, wait=False):
                    while len(self._welcome_request_queue) == 0:
                        if wait:
                            self._receive_welcome_request_event.wait()
                            self._receive_welcome_request_event.clear()
                        else:
                            return None
                    return self._welcome_request_queue.popleft()
                    
                def generateTaskIDs(self, count=1):
                    start = self._task_index
                    self._task_index += count
                    return [x for x in range(start, self._task_index)]
                
                def generateTaskID(self):
                    ID = self._task_index
                    self._task_index += 1
                    return ID
                    
                def getMappingTaskNum(self):
                    return len(self._mapping_task)
                    
                def addMappingTask(self, ID, tag, worker_fullname, task_timeout):
                    ID = int(ID)
                    self._mapping_task[ID] = {
                        'tag': tag,
                        'worker_name': worker_fullname,
                        'start_time': time.time(),
                        'timeout': task_timeout
                    }
                    #print('addMappingTask: ', self._mapping_task)
                    
                def getMappingTask(self):
                    return self._mapping_task
                    
                def getWorkersInMappingTask(self):
                    #print('getWorkersInMappingTask: ', self._mapping_task)
                    mapping_task = list(self._mapping_task.values())
                    worker_list = []
                    for m in mapping_task:
                        worker_list.append(m['worker_name'])
                    worker_list.sort()
                    return worker_list
                    
                def getMappingTaskIDList(self):
                    return list(self._mapping_task.keys())
                    
                def checkMappingTaskTimeout(self, ID):
                    ID = int(ID)
                    if ID in self._mapping_task:
                        task = self._mapping_task[ID]
                        timeout = task['timeout']
                        start_time = task['start_time']
                        now = time.time()
                        if now-start_time >= timeout:
                            return True
                    return False
                
                
                def deleteMappingTask(self, ID):
                    ID = int(ID)
                    if ID in self._mapping_task:
                        del self._mapping_task[ID]
                        
                def discardMappingTask(self, ID, default=None):
                    ID = int(ID)
                    if ID in self._mapping_task:
                        task = copy.deepcopy(self._mapping_task[ID])
                        del self._mapping_task[ID]
                        self.addTaskToBuffer(ID, task['tag'], default)
                        return task
                    return None
                
                def discardAllTimeoutTasks(self):
                    task_IDs = []
                    discarded_tasks = []
                    mapping_task = copy.deepcopy(self._mapping_task)
                    for ID in mapping_task:
                        if self.checkMappingTaskTimeout(ID):
                            self.LOG.debug('    task {} timeout'.format(ID))
                            task = self.discardMappingTask(ID)
                            discarded_tasks.append(task)
                            task_IDs.append(ID)
                    return task_IDs, discarded_tasks
                    
                def getSmallestTimeout(self):
                    _timeout = None
                    for ID in self._mapping_task:
                        timeout = self._mapping_task[ID]['start_time'] + self._mapping_task[ID]['timeout']
                        if _timeout == None:
                            _timeout = timeout
                            
                        _timeout = timeout if timeout < _timeout else _timeout
                        
                    return _timeout-time.time()
                            
                        
                
                def addTaskToBuffer(self, ID, tag, data):
                    ID = int(ID)
                    if not ID in self._task_buffer:
                        self._task_buffer[ID] = {'tag':tag, 'data': data}
            
                def getTaskBuffer(self):
                    return self._task_buffer
                
                def clearTaskBuffer(self):
                    self._task_buffer = {}
                    
                def moveTaskFromMappingToBuffer(self, ID, tag, data):
                    ID = int(ID)
                    if ID in self._mapping_task:
                        del self._mapping_task[ID]
                        self.addTaskToBuffer(ID, tag, data)
                        return True
                    return False
            
            self.helper = MessageHelper(self.LOG)
            
            self.server.servicer.setSendDataCallback(self.helper.receive_data_callback)
            self.server.servicer.setGetWelcomeMessageCallback(self.helper.receive_welcome_message_callback)
            
            # start master server
            self.server.start(port)
            
            # proxy client list
            self._proxy_client = {}
            
            # worker client list
            self._worker_client = {}
            
            # worker token list
            #    When Master Server received a message, it will look up in this list to check if it is sent from workers
            self._worker_tokens = {}
            
            
            
            self._idle_workers = []
            self._worker_num = 0
            
            self.newly_added_workers = []
            
            
            
            
            
        def _add_proxy_client(self, name, addr):
            class ProxyClientWrapper(object):
                pass
                
            proxy = ProxyClientWrapper()
            proxy.name = name
            proxy.addr = addr
            proxy.client = DefaultProxyClient(address=addr,
                                              server_name=name,
                                              logger_name=name)
                                              
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
                    
        
        
        def _add_worker_client(self, proxy_name, proxy_ip, name, port, config):
            class WorkerClientWrapper(object):
                pass
                
            worker = WorkerClientWrapper()
            worker.name = name
            worker.fullname = create_worker_fullname(proxy_name, name, port)
            worker.addr = proxy_ip + ':' + port
            
            
            worker.token = hash_name(worker.fullname)
            worker.client = DefaultWorkerClient(worker.addr)
            worker.alive = False
            worker.working = False
            worker.tasks = []
            worker.config = config
            
            
            self.LOG.debug('add worker client:')
            self.LOG.debug('    worker name: {}'.format(worker.name))
            self.LOG.debug('    worker full: {}'.format(worker.fullname))
            self.LOG.debug('    worker addr: {}'.format(worker.addr))
            self.LOG.debug('    worker token: {}'.format(worker.token))
            
            
            
            self._worker_tokens[worker.token] = worker.fullname
            
            if not proxy_name in self._worker_client:
                self._worker_client[proxy_name] = {}
                
            self._worker_num += 1
            self._worker_client[proxy_name][worker.fullname] = worker
            
            self.newly_added_workers.append(worker.fullname)
            
        def _delete_worker_client(self, fullname):
            worker_name, proxy_name, port = parse_worker_fullname(fullname)
            
            worker = self.get_worker(fullname)
            proxy = self.get_proxy(proxy_name)
            
            if worker == None or proxy == None:
                return
            
            proxy.client.shutdownWorkers([fullname])
            
            token = worker.token
            
            worker_config = copy.deepcopy(worker.config)
            
            del self._worker_tokens[worker.token]
            
            del self._worker_client[proxy_name][fullname]
            
            if fullname in self._idle_workers:
                self._idle_workers.remove(fullname)
                
            self._worker_num -= 1
            
            if fullname in self.newly_added_workers:
                self.newly_added_workers.remove(fullname)
            
            return worker_config
            
        def _create_venv(self, proxy_name, venv_name, venv_params, venv_requirements):
            if not proxy_name in self._proxy_client:
                raise Exception('unknown proxy server: {}'.format(proxy_name))
                
            self.LOG.debug('call _create_venv: proxy_name={} / venv_name={} / venv_params={} / requirements={}'.format(
                                            proxy_name, venv_name, venv_params, venv_requirements))
            
            proxy = self._proxy_client[proxy_name].client
            return proxy.createVenv(venv_name, venv_params, venv_requirements)
            
        def _create_venv_on_all_proxy(self, name, params, requirements):
            for proxy_name in self._proxy_client:
                self._create_venv(proxy_name, name, params, requirements)
        
        def _launch_workers(self, proxy_name, config):
            if not proxy_name in self._proxy_client:
                raise Exception('unknown proxy server: {}'.format(proxy_name))
            
            self.LOG.debug('call _launch_workers: proxy_name={}'.format(proxy_name))
            self._proxy_client[proxy_name].client.launchWorkers(config)
            
        
        def restart_workers(self, worker_fullnames):
        
            self.LOG.debug('call restart_workers: {}'.format(worker_fullnames))
        
            worker_fullnames = sorted(list(worker_fullnames))
            
            for fullname in worker_fullnames:
                worker_config = self._delete_worker_client(fullname)
                
                proxy_name  = worker_config['name']
                proxy_addr = worker_config['addr']
                proxy_ip, _, _ = proxy_addr.rpartition(':')
                
                name = worker_config['workers'][0]['name']
                port = worker_config['workers'][0]['port']
                
                config = {
                    'master': self.master_config,
                    'proxy': worker_config
                }
                
                self.LOG.info('    worker_config: {}'.format(config))
                
                self._add_worker_client(proxy_name, proxy_ip, name, port, worker_config)
                self._launch_workers(proxy_name, config)
            
            self.LOG.debug('    waiting for receiving welcome messages from all workers') 
            self._wait_for_receiving_welcome_message()
        
        def _wait_for_receiving_welcome_message(self, worker_list=None):
        
            if worker_list == None:
                worker_list = self.newly_added_workers
        
            while len(worker_list) > 0:
                
                token = self.helper.getWelcomeRequest(wait=True)
                
                if not token in self._worker_tokens:
                    self.LOG.warning('receive unknown token: {}'.format(token))
                    
                else:
                    worker_fullname = self._worker_tokens[token]
                    
                    _, proxy_name, _ = parse_worker_fullname(worker_fullname)
                    
                    worker = self.get_worker(worker_fullname)
                    
                    worker.alive = True
                    
                    self.LOG.debug('receive token from worker: {}'.format(worker_fullname))
                    
                    if worker_fullname in worker_list:
                        worker_list.remove(worker_fullname)
                    
        def get_proxy_num(self):
            return len(self._proxy_client)
            
        def get_worker_num(self):
            return self._worker_num
            
        def get_proxy(self, proxy_name):
            if not proxy_name in self._proxy_client:
                return None
            return self._proxy_client[proxy_name]
            
        def get_worker(self, fullname):
            worker_name, proxy_name, port = parse_worker_fullname(fullname)
            
            if worker_name == None:
                return None
            if not proxy_name in self._worker_client:
                return None
            if not fullname in self._worker_client[proxy_name]:
                return None
            
            return self._worker_client[proxy_name][fullname]
        '''
        def get_worker(self, proxy_name, worker_name, port):
            fullname = create_worker_fullname(proxy_name, worker_name, port)
            
            if not proxy_name in self._worker_client:
                return None
            if not fullname in self._worker_client[proxy_name]:
                return None
                
            return self._worker_client[proxy_name][fullname]
        '''
        def poll_worker(self, fullname):
        
            self.LOG.debug('call poll_worker: {}'.format(fullname))
            
            worker = self.get_worker(fullname)
            if worker == None:
                return None
            
            worker.alive = False if worker.client.welcome()==None else True
            if worker.alive:
                self.LOG.info('    worker still alive: {}'.format(fullname))
            else:
                self.LOG.info('    worker no response: {}'.format(fullname))
                
            return worker.alive
            
        def poll_proxy(self, proxy_name):
            self.LOG.debug('call poll_proxy: {}'.format(proxy_name))
            
            proxy = self.get_proxy(proxy_name)
            if proxy == None:
                return None
            
            proxy.alive = False if proxy.client.welcome()==None else True
            
            if proxy.alive:
                self.LOG.info('    proxy still alive: {}'.format(proxy_name))
            else:
                self.LOG.info('    proxy no response: {}'.format(proxy_name))
                
            return proxy.alive
            
        def poll_all_servers(self):
            self.LOG.debug('call poll_all_servers:')
            
            for proxy_name in self._proxy_client:
                proxy = self._proxy_client[proxy_name]
                proxy.alive = False if proxy.client.welcome()==None else True
                
                if proxy.alive:
                    self.LOG.debug('    proxy still alive: {}'.format(proxy_name))
                else:
                    self.LOG.info('    proxy no response: {}'.format(proxy_name))
                
                if proxy_name in self._worker_client:
                    for worker_name in self._worker_client[proxy_name]:
                        worker = self._worker_client[proxy_name][worker_name]
                        worker.alive = False if worker.client.welcome()==None else True
                        if worker.alive:
                            self.LOG.debug('    worker still alive: {}'.format(worker_name))
                        else:
                            self.LOG.debug('    worker no response: {}'.format(worker_name))
                            
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
        
        def check_worker_alive(self, fullname):
            self.LOG.debug('call check_worker_alive: {}'.format(fullname))
            worker = self.get_worker(fullname)
            if worker == None:
                return None
            
            return worker.alive
            
        def check_proxy_alive(self, proxy_name):
            self.LOG.debug('call check_proxy_alive: {}'.format(proxy_name))
            proxy = self.get_proxy(proxy_name)
            if proxy == None:
                return None
                
            return proxy.alive
            
        def update_idle_worker_list(self):
            idle_list = []
            for proxy_name in self._worker_client:
                for worker_name in self._worker_client[proxy_name]:
                    worker = self._worker_client[proxy_name][worker_name]
                    
                    if worker.alive and not worker.working:
                        idle_list.append( worker.fullname )
                        
            self._idle_list = idle_list
        
        def _send_task(self, worker, data, tag):
            fullname = worker.fullname
            ID = self.helper.generateTaskID()
            if tag == None:
                tag = ''
                
            task_tag = '{}:{}:{}'.format('task', ID, tag)
            
            self.LOG.debug('    send task to worker: task_ID={} / worker={}'.format(ID, fullname))
            
            pickle_data = dill.dumps(data)
            
            res = worker.client.sendData(pickle_data, task_tag)
            
            return res, ID 
            
        
        def map(self, data_list, tag_list=None, task_timeout=3*60*60): 
            self.LOG.debug('call map: ')
            
            if tag_list == None:
                tag_list = [None for _ in range(len(data_list))]
            
            if len(data_list) == 0:
                self.LOG.error('in Master.map, data_list is empty')
                return False
            
            #if self.helper.getMappingTaskNum() > 0:
            #    self.LOG.error('in Master.map, workers are still in mapping stage, please reduce first')
            #    return False
                
            if len(tag_list) != len(data_list):
                self.LOG.error('in Master.map, the size of data_list does not match the size of tag_list')
                return False
            
            
            self.update_idle_worker_list()
            
            data_iter = iter(zip(data_list, tag_list))

            idle_list = self._idle_list.copy()
            
            try:
                for fullname in idle_list:
                    
                    data, tag = next(data_iter)
                    worker = self.get_worker(fullname)
                    
                    res, ID = self._send_task(worker, data, tag)
                    
                    if res:
                        self.helper.addMappingTask(ID, tag, fullname, task_timeout)
                        
                        worker.tasks.append(int(ID))
                        worker.working = True
                        self._idle_list.remove(fullname)
                
                for proxy_name in self._worker_client:
                    for worker_name in self._worker_client[proxy_name]:
                        worker = self._worker_client[proxy_name][worker_name]
                        
                        if not worker.alive:
                            continue
                        
                        data, tag = next(data_iter)
                        
                        res, ID = self._send_task(worker, data, tag)
                        
                        if res:
                            self.helper.addMappingTask(ID, tag, worker.fullname, task_timeout)
                            worker.tasks.append(int(ID))
                            worker.working = True
                            
            
            except StopIteration:
                pass
                
            return True
            
            
        def reduce(self, block=True):
        
            self.LOG.debug('call reduce: {} tasks / block = {}'.format(self.helper.getMappingTaskNum(), block))
            
            timeout_worker_list = []
            
            while self.helper.getMappingTaskNum() > 0:
                start_time = time.time()
            
                timeout = self.helper.getSmallestTimeout()
                
                timeout = 0 if timeout < 0 else timeout
                
                timeout_time = get_current_time_plus_delta(timeout)
                
                self.LOG.debug('    timeout: {} sec / next timeout at: {}'.format(timeout, timeout_time))
            
                d = self.helper.getTask(wait=block, timeout=timeout)
                
                
                
                # if no task results received and non-blocking
                if d == None:
                
                    # timeout
                    if time.time() > start_time + timeout:
                    
                        self.LOG.debug('    timeout!')
                        
                        # handle timeout task
                        IDs, tasks = self.helper.discardAllTimeoutTasks()
                        
                        self.LOG.debug('    tasks: {} will be discarded and the results will be supplanted by None'.format(tasks))
                        
                        # save timeout workers to the list
                        for task in tasks:
                            timeout_worker_list.append(task['worker_name'])
                            
                        continue
                    
                    # not timeout
                    else:
                        if not block:
                            return False, None
                        else:
                            continue
                    
                ID = d['id']
                tag = d['tag']
                token = d['token']
                data = d['data']
                
                # receive data
                if token in self._worker_tokens:
                    worker_fullname = self._worker_tokens[token]
                    
                    self.LOG.debug('    receive result from worker: ID={} / tag={} / worker={} / data={}'.format(ID, tag, worker_fullname, data))
                    
                    res = self.helper.moveTaskFromMappingToBuffer(ID, tag, data)
                    
                    if res == True:
                        worker = self.get_worker(worker_fullname)
                        worker.alive = True
                        worker.working = False
                    
                        worker.tasks.remove(int(ID))
                    
                else:
                    self.LOG.debug('    receive result from unknown worker: ID={} / tag={} / token={} / data={}'.format(ID, tag, token, data))
                    self.LOG.debug('        discard')
                    
                    self.helper.deleteMappingTask(ID)
                
                # not done
                if self.helper.getMappingTaskNum() > 0:
                    worker_list = self.helper.getWorkersInMappingTask()
                    worker_dict = {}
                    for w_fullname in worker_list:
                        worker_name, proxy_name, port = parse_worker_fullname(w_fullname)
                        if not proxy_name in worker_dict:
                            worker_dict[proxy_name] = []
                        
                        worker_dict[proxy_name].append('{}:{}'.format(worker_name, port))
                        
                    for proxy in worker_dict:
                        self.LOG.info('on proxy {}, workers: {} are still working'.format(proxy, worker_dict[proxy]))
            
            # restart workers
            if len(timeout_worker_list) > 0:
                timeout_workers = list(set(timeout_worker_list))
                self.LOG.debug('    restart workers: {}'.format(timeout_workers))
                self.restart_workers(timeout_workers)
            
            # all done
            if self.helper.getMappingTaskNum() == 0:
                self.LOG.debug('    ALL DATA RECEIVED')
                task_buffer = self.helper.getTaskBuffer()
                keys = list(task_buffer.keys())
                keys.sort()
                
                result_list = []
                
                for key in keys:
                    d = task_buffer[key]
                    result_list.append(d['data'])  #CAUTION
                    
                self.LOG.debug('        {}'.format(result_list))
                
                self.helper.clearTaskBuffer()
                
                return True, result_list

            return True, None # ret, results

        def close(self):
            try:
                self.LOG.debug('call close:')
            except:
                pass
            
            for proxy_name in self._worker_client:
                    
                proxy = self._proxy_client[proxy_name].client
                
                proxy.shutdownAllWorkers()

                try:
                    self.LOG.debug('    send shutdownWorkers request to proxy: {}'.format(proxy_name))
                except:
                    pass
                
        def __del__(self):
            self.close()
    
    # ---- worker
    
    class Worker(object):
        def __init__(self, fullname, port, master_name, master_addr,
                          max_workers=5,
                          logger_name='Cluster-Worker',
                          logger_level='DEBUG',
                          log_to_file=False):
                          
            loadConfig()
            self.LOG = createLoggerFromExistedLogger(logger_name, logger_level, log_to_file=log_to_file)
            
            _worker_name, _proxy_name, _ = parse_worker_fullname(fullname)
            
            self.name = _worker_name
            self.fullname = fullname
            self.port = port
            
            self.token = hash_name(self.fullname)
            self.LOG.debug('{}'.format(self.token))
            
            self.server = DefaultWorkerServer(max_workers=max_workers)
            self.server.start(self.port)
            
            self.master = DefaultMasterClient(master_addr)
            
            self.LOG.debug('worker name: {}'.format(self.name))
            self.LOG.debug('worker port: {}'.format(self.port))
            self.LOG.debug('master name: {}'.format(master_name))
            self.LOG.debug('master addr: {}'.format(master_addr))

            self.task_queue = collections.deque()
            self.data_queue = collections.deque()
            self.receive_task_event = threading.Event()
            self.working_tasks = []
            
            def receive_data_callback(request, context):
                data = request.data
                tag = request.tag
                
                #TODO: unpickle data
                unpickle_data = dill.loads(data)
                
                _type, _ID, _tag = parse_task_tag(tag)
                
                d = {'id': _ID, 'tag': _tag, 'data': unpickle_data}
                if _type == 'task':
                    self.task_queue.append(d)
                    self.receive_task_event.set()
                else:
                    self.data_queue.append(d)
                    self.receive_task_event.set()
                    
            self.server.servicer.setSendDataCallback(receive_data_callback)
            
        def master_welcome(self):
            self.LOG.debug('call master_welcome:')
            
            res = self.master.welcome(self.token)
            if res != None:
                self.LOG.debug('    receive welcome info from master: {}'.format(res))
                
        def wait_for_task(self):
            self.LOG.debug('call wait_for_task:')
            
            while len(self.task_queue) == 0:
                self.receive_task_event.wait()
                self.receive_task_event.clear()
            
        def get_task(self):
            self.LOG.debug('call get_task:')
        
            d = self.task_queue.popleft()
            
            self.working_tasks.append(d['id'])
            
            return d['id'], d['tag'], d['data']
            
        def submit(self, data, ID=None, tag=None):
            self.LOG.debug('call submit: ID={}'.format(ID))
            
            if len(self.working_tasks) == 0:
                self.LOG.error('no task could be submitted!')
                return False
            
            if len(self.working_tasks) == 1:
                ID = self.working_tasks[-1]
                
            
            if not ID in self.working_tasks:
                self.LOG.error('submit unknown task ID: {} / valid task IDs: {}'.format(ID, self.working_tasks))
                return False
            
            self.working_tasks.remove(ID)
            
            pickle_data = dill.dumps(data)
            
            if tag == None:
                tag = ''
                
            task_tag = '{}:{}:{}'.format('task', ID, tag)
            
            return self.master.sendData(data=pickle_data, tag=task_tag, token=self.token)
            
        def set_data_request_callback(self, tag, callback):
            self.server.servicer.setGetMessagesCallback(tag, callback)
            
        def close(self):
            try:
                self.server.stop()
            except:
                pass
                
        def __del__(self):
            self.close()
            
            
    # --- Cluster
    def __init__(self, config_file,
                       max_workers=5,
                       logger_name = 'Cluster',
                       logger_level= 'DEBUG'):
                       
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
            
        self.max_workers = max_workers
        

        self._config = config
        
        self.LOG.debug('creating cluster')
        self._create_cluster()
        
        self.LOG.debug('creating virtual environment')
        self._create_venvs()
        
        self.LOG.debug('launching workers')
        self._launch_workers()
        
        self.LOG.debug('checking servers\' status')
        self._check_all_alive()
        
    @staticmethod
    def worker(max_workers=5):
        worker_root = os.environ['CLUSTER_WORKER_ROOT']
        worker_name = os.environ['CLUSTER_WORKER_NAME']
        worker_port = os.environ['CLUSTER_WORKER_PORT']
        master_name = os.environ['CLUSTER_MASTER_NAME']
        master_addr = os.environ['CLUSTER_MASTER_ADDR']
        
        '''
        if root_directory != None:
            worker_root = root_directory

        if worker_root == None:
            worker_root = os.path.abspath('./')

        import sys
        sys.path.append(os.getcwd())
        os.chdir(worker_root)
        '''

        _worker = Cluster.Worker(worker_name, worker_port, master_name, master_addr, max_workers)
        
        _worker.master_welcome()
        
        return _worker
        
    def _parse_master_config(self, _master_config):
        # master
        assert _master_config != None
        
        _master_name = _master_config.get('name')
        _master_addr = _master_config.get('addr')
        
        assert _master_name != None
        assert _master_addr != None
        
        self.LOG.debug('    master name: {}'.format(_master_name))
        self.LOG.debug('    master addr: {}'.format(_master_addr))
        
        self._master_config = _master_config
        self._master_name = _master_name
        self._master_addr = _master_addr
        self._master_ip, _, self._master_port = self._master_addr.rpartition(':')
    
    def _parse_proxy_config(self, _proxy_config):
        
        proxy_name = _proxy_config.get('name')
        proxy_addr = _proxy_config.get('addr')
        assert proxy_name != None
        assert proxy_addr != None
        
        self.LOG.debug('    proxy name: {}'.format(proxy_name))
        self.LOG.debug('    proxy addr: {}'.format(proxy_addr))
        
        proxy_ip, _, _ = proxy_addr.rpartition(':')
        return proxy_name, proxy_addr, proxy_ip
    
    def _parse_worker_config(self, _worker_config):
        worker_name = _worker_config.get('name')
        worker_port = _worker_config.get('port')
        assert worker_name != None
        assert worker_port != None

        worker_name = str(worker_name)
        worker_port = str(worker_port)

        if '-' in worker_port:
            start_port, end_port = worker_port.split('-')
            start_port = int(start_port.strip())
            end_port = int(end_port.strip())
            return worker_name, [str(x) for x in range(start_port, end_port+1)]
        else:
            return worker_name, [worker_port]
    
    def _create_master_server(self, name, port, master_config, max_workers):
        self._master_server = Cluster.Master(name, port, master_config, max_workers)
    
    def _create_proxy_client(self, proxy_name, proxy_addr):
        self._master_server._add_proxy_client(proxy_name, proxy_addr)
    
    def _create_worker_client(self, proxy_name, proxy_ip, worker_name, worker_port, worker_full_config):
        self._master_server._add_worker_client(proxy_name, proxy_ip, worker_name, worker_port, 
                                                                worker_full_config)
    
    def _create_cluster(self):
        
        self.LOG.debug('call _create_cluster: ')
        
        # master
        self._parse_master_config(self._config.get('master'))
        
        # create master server
        self._create_master_server(self._master_name, self._master_port, self._master_config, self.max_workers)
        
        # proxy
        self._proxy_config = self._config.get('proxy')
        
        
        for proxy_config in self._proxy_config:
            
            proxy_name, proxy_addr, proxy_ip = self._parse_proxy_config(proxy_config)
            
            # create proxy client
            self._create_proxy_client(proxy_name, proxy_addr)
            
            # check if it is alive
            if self._master_server.poll_proxy(proxy_name) != True:
                raise Exception('failed to connect to proxy server: {} addr: {}'.format(proxy_name, proxy_addr))
            
            
            if proxy_config.get('workers') != None:
                for worker_config in proxy_config.get('workers'):
                
                    worker_name, worker_ports = self._parse_worker_config(worker_config)
                

                    def _launch_single_worker_client(worker_name, worker_port):
                    
                        self.LOG.debug('    worker name: {}'.format(worker_name))
                        self.LOG.debug('    worker port: {}'.format(worker_port))
                    
                    
                        assert worker_config.get('entrypoint') != None or proxy_config.get('default_entrypoint') != None
                        
                        worker_full_config = copy.deepcopy(proxy_config)
                        worker_full_config['workers'] = [copy.deepcopy(worker_config)]
                        worker_full_config['workers'][0]['name'] = worker_name
                        worker_full_config['workers'][0]['port'] = worker_port
                    
                        # create worker client
                        self._create_worker_client(proxy_name, proxy_ip, worker_name, worker_port, worker_full_config)
                    
                    
                    for port in worker_ports:
                        _worker_name = worker_name
                        _worker_port = port
                        _launch_single_worker_client(_worker_name, _worker_port)
                    
                        
                        
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
            
            self._master_server._launch_workers(name, configure)
           
        self.LOG.debug('    waiting for receiving welcome messages from all workers') 
        self._master_server._wait_for_receiving_welcome_message(None)
    
    def _check_all_alive(self):
        self.LOG.debug('call _check_all_alive:')
        
        self._master_server.poll_all_servers()
        
        return self._master_server.check_all_alive()
        
        
    def get_proxy_num(self):
        return self._master_server.get_proxy_num()
        
    def get_worker_num(self):
        return self._master_server.get_worker_num()
        
    def map(self, data_list, tag_list = None, timeout=3*60*60):
        self.LOG.debug('call map:')
        
        return self._master_server.map(data_list, tag_list=tag_list, task_timeout=timeout)
        
    def reduce(self, block=True):
        self.LOG.debug('call reduce:')
        
        done, result_list = self._master_server.reduce(block=block)
        if block:
            return done, result_list
        else:
            return done, result_list
            
    def close(self):
        try:
            self.LOG.debug('call close:')
            
            self._master_server.close()
            
        except:
            pass
            
    def __del__(self):
        self.close()
