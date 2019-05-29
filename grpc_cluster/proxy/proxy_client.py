import os
import traceback

from google.protobuf import empty_pb2

from grpc_cluster.common import common_type
from grpc_cluster.common import ExceptionWrapper as Ewrapper

import grpc
from grpc_cluster.proxy import proxy_pb2 as pb2
from  grpc_cluster.proxy import proxy_pb2_grpc as pb2_grpc

import logging
from grpc_cluster.logger import *

CHUNK_SIZE = 1024*1024


class LowEndProxyClient:
    def __init__(self, address):
        channel = grpc.insecure_channel(address)
        self.stub = pb2_grpc.ProxyStub(channel)
        self.token = None

        loadConfig()
        self.LOG = createLoggerFromExistedLogger('LowEndProxyClient', 'DEBUG')

    # already tested
    def _uploadFileInfo(self, name, target):
        size = os.path.getsize(name)
        
        fileinfo = common_type.FileInfo(name=target, size=size)

        request = common_type.UploadFileInfoRequest(token=self.token, info=fileinfo)

        response = self.stub.uploadFileInfo(request)
        
        return response

    # already tested
    def _uploadFile(self, key, name):
        def chunk_iter(filename):
            with open(filename, 'rb') as f:
                while True:
                    piece = f.read(CHUNK_SIZE)
                    if len(piece) == 0:
                        return
                    chunk = common_type.FileChunk(key=key, buffer=piece)
                    yield common_type.UploadFileRequest(chunk=chunk)

        response = self.stub.uploadFile(chunk_iter(name))

        return response

    # already tested
    def _downloadFile(self, target, name):
        fileinfo = common_type.FileInfo(name=target)
        request = common_type.DownloadFileRequest(token=self.token, info=fileinfo)
        responses = self.stub.downloadFile(request)
        
        filepath = os.path.dirname(name)
        if not os.path.exists(filepath) and filepath != '':
            os.makedirs(filepath)

        with open(name, 'wb') as f:
            for response in responses:
                chunk = response.chunk
                f.write(chunk.buffer)
                end_response = response
        return end_response

    def _getDirectoryList(self):
        raise NotImplementedError('Method not implemented!')
    
    def _getFileInfo(self):
        raise NotImplementedError('Method not implemented!')
    
    def _getFileInfo(self):
        raise NotImplementedError('Method not implemented!')
    
    def _moveFileOrDirectory(self):
        raise NotImplementedError('Method not implemented!')
    
    def _removeFileOrDirectry(self):
        raise NotImplementedError('Method not implemented!')
    
    def _executeCommand(self, command, timeout=0):
        cmd = common_type.Command(value=command, timeout=timeout)

        request = common_type.ExecuteCommandRequest(token=self.token, command=cmd)

        response = self.stub.executeCommand(request)
        
        return response

    # already tested
    def _register(self, username, password, admin_token=None):
        
        token = admin_token if self.token == None else self.token
        
        request = common_type.RegisterRequest(token=token, username=username, password=password)

        response = self.stub.register(request)
        
        self.token = response.token

        return response

    # already tested
    def _signIn(self, username, password):

        request = common_type.SignInRequest(username=username, password=password)

        response = self.stub.signIn(request)
        
        self.token = response.token

        return response

    # already tested
    def _checkUserExistance(self, username):
        request = common_type.CheckUserExistanceRequest(username = username)

        response = self.stub.checkUserExistance(request)

        return response

    # already tested
    def _sendMessage(self, message):

        request = common_type.SendMessageRequest(token=self.token, msg=message)

        response = self.stub.sendMessage(request)

        return response

    # already tested
    def _getWelcomeMessage(self):

        request = common_type.Token()

        response = self.stub.getWelcomeMessage(request)

        return response

    def _launchWorkers(self, config):

        config_master = config.get('master')
        master_name = config_master.get('name')
        master_addr = config_master.get('addr')
        master = common_type.ClusterConfiguration.Master(name=master_name, addr=master_addr)
        
        config_proxy = config.get('proxy')
        proxy_name = config_proxy.get('name')
        proxy_default_entrypoint = config_proxy.get('default_entrypoint')
        proxy_default_venv = config_proxy.get('default_venv')

        workers = []

        for config_worker in config_proxy.get('workers'):
            
            # create Worker list
            
            worker_name = config_worker.get('name')
            worker_port = config_worker.get('port')
            worker_entrypoint = config_worker.get('entrypoint')
            worker_venv = config_worker.get('venv')

            envs = []
            
            worker_env = config_worker.get('environment')

            if worker_env != None:
            	for config_worker_env in config_worker.get('environment'):
                    # create environment variable list
                    variable, value = [x.strip() for x in config_worker_env.split('=')]
                    env = common_type.ClusterConfiguration.Proxy.Worker.Environment(variable=variable, value=value)
                
                    envs.append(env) # repeated Environment

            def create_worker_config(name, port,
                          entrypoint, venv, environment):

                worker = common_type.ClusterConfiguration.Proxy.Worker(
                    name=name,
                    port=port,
                    entrypoint=entrypoint,
                    venv=venv,
                    environment=environment)

                return worker
                


            if '-' in worker_port:
                start_port, end_port = worker_port.split('-')

                start_port = int(start_port.strip())
                end_port = int(end_port.strip())

                for port in range(start_port, end_port+1):
                    _worker_name = worker_name
                    _worker_port = str(port)
 
                    worker = create_worker_config(_worker_name, _worker_port,
                             worker_entrypoint, worker_venv, envs)

                    workers.append(worker)

            else:
                worker = create_worker_config(worker_name, worker_port,
                             worker_entrypoint, worker_venv, envs)

                workers.append(worker)
       

        proxy = common_type.ClusterConfiguration.Proxy(
                    name=proxy_name,
                    default_entrypoint=proxy_default_entrypoint,
                    default_venv=proxy_default_venv,
                    workers=workers)

        configuration = common_type.ClusterConfiguration(
                master=master,
                proxy=proxy)

        request = common_type.LaunchWorkerRequest(
                token=self.token,
                configuration=configuration)

        # send request
        response = self.stub.launchWorkers(request)

        return response

    def _forceShutdownWorkers(self, unique_names=None, shutdown_all=False):
        request = common_type.ForceShutdownWorkersRequest(token=self.token, all=shutdown_all, fullname=unique_names)
        
        response = self.stub.forceShutdownWorkers(request)
        
        return response

    # already tested
    def _createVirtualenv(self, config):
        path = config.get('path')
        params = config.get('params')
        requirements = config.get('requirements')
        
        assert path != None or path != ''
        
        configuration = common_type.VirtualenvConfiguration(path=path, params=params,
								requirements=requirements)

        request = common_type.CreateVirtualenvRequest(token=self.token, configuration=configuration)

        # send request
        response = self.stub.createVirtualenv(request)

        return response


class DefaultProxyClient(LowEndProxyClient):
    def __init__(self, address,
                       server_name,
                       logger_name='DefaultProxyClient',
                       logger_level='DEBUG'):
    
        LowEndProxyClient.__init__(self=self, address=address)
        
        self.server_name = server_name
        
        loadConfig()
        self.LOG = createLoggerFromExistedLogger(logger_name, logger_level)
    
    def _assertStatus(self, res):
        if res.status != common_type.Status.Value('OK'):
            raise Exception( res.error )
        
    
    def _handleError(self, msg, e):
        self.LOG.error('exception occurred in {}: {}'.format(msg, str(e)))
        self.LOG.error('  traceback: ')
        self.LOG.error('{}'.format(traceback.format_exc()))
        
    def upload(self, name, target):
        try:
            res = self._uploadFileInfo(name, target)
            
            self._assertStatus(res)
            
            res = self._uploadFile(res.key, name)
            
            self._assertStatus(res)
        except Exception as e:
            self._handleError('upload', e)
            return False
            
        return True
        
    def download(self, target, name):
        try:
            res = self._downloadFile(target, name)
            
            self._assertStatus(res)
        except Exception as e:
            self._handleError('download', e)
            return False
        return True
    
    def execute(self, command, timeout=0):
        try:
            res = self._executeCommand(command, timeout=timeout)
            
            self._assertStatus(res)
            
        except Exception as e:
            self._handleError('execute', e)
            return False
            
        return True
        
    def register(self, username, password, admin_token=None):
        try:
            res = self._register(username, password, admin_token=admin_token)
            
            self._assertStatus(res)
            
            return res.token
        except Exception as e:
            self._handleError('register', e)
            return None
            
            
    def login(self, username, password):
    
        try:
            res = self._signIn(username, password)
            
            self._assertStatus(res)
            
            return res.token
        except Exception as e:
            self._handleError('login', e)
            return None
           
           
    def checkUser(self, username):
        try:
            res = self._checkUserExistance(username)
            
            self._assertStatus(res)
            
            return 'True' == res.result
        except Exception as e:
            self._handleError('checkUser', e)
            return False
            
    def sendMessage(self, message):
        try:
            res = self._sendMessage(message)
            
            self._assertStatus(res)
            
        except Exception as e:
            self._handleError('sendMessage', e)
            return False    
        return True
        
    def welcome(self):
        try:
            res = self._getWelcomeMessage()
            
            self._assertStatus(res)
            
            return res.result
        except Exception as e:
            self._handleError('welcome', e)
        
            return None
    
    def launchWorkers(self, config):
        try:
            
            res = self._launchWorkers(config)
            
            self._assertStatus(res)
            
            return True
        except Exception as e:
            self._handleError('launchWorkers', e)
            
            return False
            
    def shutdownAllWorkers(self):
        try:
            res = self._forceShutdownWorkers(shutdown_all=True)
            self._assertStatus(res)
            
            return True
        except Exception as e:
            self._handleError('shutdownAllWorkers', e)
            
            return False
    
    
    def shutdownWorkers(self, unique_names):
        try:
            res = self._forceShutdownWorkers(unique_names=unique_names)
            self._assertStatus(res)
            
            return True
    
        except Exception as e:
            self._handleError('shutdownWorkers', e)
            
            return False
    
    def createVenv(self, path, params, requirements):
        try:
            assert path != None or path != ''
            
            config = {
                'path': path,
		        'params': params,
                'requirements': requirements
            }
            
            res = self._createVirtualenv(config)
            
            self._assertStatus(res)
            
            return True
        except Exception as e:
            self._handleError('createVenv', e)
            
            return False
            
