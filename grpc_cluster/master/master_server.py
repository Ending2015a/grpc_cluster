import os
import time
import traceback
from concurrent import futures


from grpc_cluster.common import common_type
from grpc_cluster.common import ExceptionWrapper as Ewrapper

import grpc
from grpc_cluster.master import master_pb2_grpc as pb2_grpc
from grpc_cluster.master import master_pb2 as pb2

import logging
from grpc_cluster.logger import *

class DefaultMasterServicer(pb2_grpc.MasterServicer):
    def __init__(self, 
                logger_name='DefaultMasterServicer',
                logger_level='DEBUG'):
        
        self.sendDataCallback = None
        self.sendMessageCallback = None
        self.welcome_message = 'Hello client'
        
        loadConfig()
        self.LOG = createLoggerFromExistedLogger(logger_name, logger_level)  
    
    def _getStatusObject(self, status='OK'):
        # 'UNKNOWN' / 'OK' / 'FAILED'
        assert status == 'OK' or status == 'UNKNOWN' or status == 'FAILED'
        return common_type.Status.Value(status)
    
    def _reportError(self, msg, e):
        # 
        # write exception details to logger
        self.LOG.error('{}: {}'.format(msg, str(e)))
        self.LOG.error('  traceback: ')
        self.LOG.error('{}'.format(traceback.format_exc()))

    def _handleError(self, scope_name, e, code=None, errmsg=None):
        # TODO:
        # report error and return failed status and error code
        self._reportError('exception occurred in {}'.format(scope_name), e)
        status = self._getStatusObject('FAILED')
        errcode = Ewrapper.exceptionToErrCode(e, code, errmsg)
        return status, errcode
    
    
    def setSendDataCallback(self, callback):
        self.sendDataCallback = callback
    
    def setSendMessageCallback(self, callback):
        self.sendMessageCallback = callback
                
    def sendData(self, request, context):
        # Input: common_type.SendDataRequest
        # Output: common_type.StatusResponse

        self.LOG.debug('receive sendMessage request')

        #token = request.token
        data = request.data

        try:
            #self.authenticateToken(token) #throw exception

            #username = self.getUsernameByToken(token)
            
            

            self.LOG.info('[user unknown] send {} bytes of data'.format(len(data)))
            

            if self.sendDataCallback:
                self.LOG.debug('calling sendDataCallback function')
                self.sendDataCallback(request, context)

            status = self._getStatusObject('OK')
            response = common_type.StatusResponse(status=status)

        except Exception as e:
            status, errcode = self._handleError('sendData', e)
            response = common_type.StatusResponse(status=status, error=errcode)
        
        return response
    
    def sendMessage(self, request, context):
        # Input: common_type.SendMessageRequest
        # Output: common_type.StatusResponse

        self.LOG.debug('receive sendMessage request')

        #token = request.token
        msg = request.msg

        try:
            #self.authenticateToken(token) #throw exception

            #username = self.getUsernameByToken(token)
            

            self.LOG.info('[user unknown]: {}'.format(msg))
            
            if self.sendMessageCallback:
                self.LOG.debug('calling sendMessageCallback function')
                self.sendMessageCallback(request, context)

            status = self._getStatusObject('OK')
            response = common_type.StatusResponse(status=status)

        except Exception as e:
            status, errcode = self._handleError('sendMessage', e)
            response = common_type.StatusResponse(status=status, error=errcode)
        
        return response
        
    def getWelcomeMessage(self, request, context):
        # Input: common_type.Token
        # Output: common_type.StatusResponse

        self.LOG.debug('receive getWelComeMessage request')

        try:
            
            status = self._getStatusObject('OK')

            response = common_type.StatusResponse(result=self.welcome_message, status=status)

        except Exception as e:
            status, errcode = self._handleError('getWelcomeMessage', e)
            response = common_type.StatusResponse(status=status, error=errcode)

        return response
        

class DefaultMasterServer(object):
    def __init__(self, max_workers=5, servicer=DefaultMasterServicer):
        
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
        
        self.servicer = servicer()
        
        pb2_grpc.add_MasterServicer_to_server(self.servicer, self.server)
        
    def start(self, port, block=False):
        self.server.add_insecure_port('[::]:{}'.format(port))
        
        self.server.start()
        print('server listening at port: {}'.format(port))
        
        try:
            while block:
                time.sleep(60*60*24)
                
        except KeyboardInterrupt:
            self.server.stop(0)
            
    def stop(self):
        self.server.stop(0)
            
            
