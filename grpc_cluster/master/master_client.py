import os
import time
import traceback


from grpc_cluster.common import common_type
from grpc_cluster.common import ExceptionWrapper as Ewrapper

import grpc
from grpc_cluster.master import master_pb2 as pb2
from grpc_cluster.master import master_pb2_grpc as pb2_grpc

import logging
from grpc_cluster.logger import *


class LowEndMasterClient:
    def __init__(self, address):
        channel = grpc.insecure_channel(address)
        self.stub = pb2_grpc.MasterStub(channel)
        
        loadConfig()
        self.LOG = createLoggerFromExistedLogger('LowEndProxyClient', 'DEBUG')
    
    
    def _sendData(self, data, tag=None, token=None):
        token = common_type.Token(value=token)
        request = common_type.SendDataRequest(token=token, tag=tag, data=data)
        
        response = self.stub.sendData(request)
        
        return response
        
    def _sendMessage(self, message, tag=None, token=None):
        token = common_type.Token(value=token)
        request = common_type.SendMessageRequest(token=token, tag=tag, msg=message)
        
        response = self.stub.sendMessage(request)
        
        return response
        
    def _getWelcomeMessage(self, token=None):
        token = common_type.Token(value=token)
        request = token
        
        response = self.stub.getWelcomeMessage(request)
        
        return response

class DefaultMasterClient(LowEndMasterClient):
    def __init__(self, address,
                        logger_name='DefaultMasterClient',
                        logger_level='DEBUG'):
                        
        LowEndMasterClient.__init__(self=self, address=address)
        
        
        loadConfig()
        self.LOG = createLoggerFromExistedLogger(logger_name, logger_level)
    
    def _assertStatus(self, res):
        if res.status != common_type.Status.Value('OK'):
            raise Exception( res.error )
        
    
    def _handleError(self, msg, e):
        self.LOG.error('exception occurred in {}: {}'.format(msg, str(e)))
        self.LOG.error('  traceback: ')
        self.LOG.error('{}'.format(traceback.format_exc()))
    
    
    def sendData(self, data, tag=None, token=None):
        try:
            res = self._sendData(data, tag, token)
            
            self._assertStatus(res)
        except Exception as e:
            self._handleError('sendData', e)
            return False
        return True
        
        
    def sendMessage(self, message, tag=None, token=None):
        try:
            res = self._sendMessage(message, tag)
            
            self._assertStatus(res)
        except Exception as e:
            self._handleError('sendMessage', e)
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

