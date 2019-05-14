import os
import time
import traceback


from grpc_cluster.common import common_type
from grpc_cluster.common import ExceptionWrapper as Ewrapper

import grpc
from grpc_cluster.worker import worker_pb2 as pb2
from grpc_cluster.worker import worker_pb2_grpc as pb2_grpc
from grpc_cluster.worker import worker_type_pb2 as worker_type


import logging
from grpc_cluster.logger import *



class LowEndWorkerClient:
    def __init__(self, address):
        channel = grpc.insecure_channel(address)
        self.stub = pb2_grpc.WorkerStub(channel)
        
        loadConfig()
        self.LOG = createLoggerFromExistedLogger('LowEndWorkerClient', 'DEBUG')
    
    
    def _sendData(self, data, tag=None):
        request = common_type.SendDataRequest(tag=tag, data=data)
        
        response = self.stub.sendData(request)
        
        return response
        
    def _sendMessage(self, message, tag=None):
        request = common_type.SendMessageRequest(tag=tag, msg=message)
        
        response = self.stub.sendMessage(request)
        
        return response
        
        
    def _getMessages(self, tags):
        assert type(tags) != str and len(tags) > 0
        
        request = worker_type.GetMessagesRequest(tags=tags)
        
        response = self.stub.getMessages(request)
        
        return response
        
    def _shutdown(self):
        request = worker_type.ShutdownRequest()
        
        response = self.stub.shutdown(request)
        
        return response
        
    def _getWelcomeMessage(self, token=None):
        request = common_type.Token(value=token)
        
        response = self.stub.getWelcomeMessage(request)
        
        return response
        

class DefaultWorkerClient(LowEndWorkerClient):
    def __init__(self, address,
                    logger_name='DefaultWorkerClient',
                    logger_level='DEBUG'):
                    
        LowEndWorkerClient.__init__(self=self, address=address)
        
        loadConfig()
        self.LOG = createLoggerFromExistedLogger(logger_name, logger_level)
        
    def _assertStatus(self, res):
        if res.status != common_type.Status.Value('OK'):
            raise Exception( res.error )
        
    
    def _handleError(self, msg, e):
        self.LOG.error('exception occurred in {}: {}'.format(msg, str(e)))
        self.LOG.error('  traceback: ')
        self.LOG.error('{}'.format(traceback.format_exc()))
        
    def sendData(self, data, tag=None):
        try:
            res = self._sendData(data, tag)
            
            self._assertStatus(res)
        except Exception as e:
            self._handleError('sendData', e)
            return False
        return True
        
        
    def sendMessage(self, message, tag=None):
        try:
            res = self._sendMessage(message, tag)
            
            self._assertStatus(res)
        except Exception as e:
            self._handleError('sendMessage', e)
            return False
        return True
        
    def getMessages(self, tags):
        try:
            res = self._getMessages(tags)
            
            self._assertStatus(res)
        except Exception as e:
            self._handleError('getMessages', e)
            return None
        return res.results
        
    def getMessage(self, tag):
        try:
            res = self._getMessages([tag])
            
            self._assertStatus(res)
        except Exception as e:
            self._handleError('getMessage', e)
            return None
        if res.results == None:
            return None
        return res.results[0]
        
    def shutdown(self):
        try:
            res = self._shutdown()
            
            self._assertStatus(res)
        except Exception as e:
            self._handleError('shutdown', e)
            return False
        return True
    
    def welcome(self, token=None):
        try:
            res = self._getWelcomeMessage(token=token)
            
            self._assertStatus(res)
            return res.result
        except Exception as e:
            self._handleError('welcome', e)
            return None
