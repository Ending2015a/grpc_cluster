import os
import time
import shlex
import _thread
import threading
import traceback
from concurrent import futures

# common message types
from grpc_cluster.common import common_type
from grpc_cluster.common import ExceptionWrapper as Ewrapper


import grpc
from grpc_cluster.worker import worker_pb2_grpc as pb2_grpc
from grpc_cluster.worker import worker_pb2 as pb2
from grpc_cluster.worker import worker_type_pb2 as worker_type

import logging
from grpc_cluster.logger import *

from grpc_cluster.common.auth import AuthenticationServicer


class DefaultWorkerServicer(pb2_grpc.WorkerServicer):


    def __init__(self, 
                logger_name='DefaultWorkerServicer',
                logger_level='DEBUG'):
                
        self.sendDataCallback = None
        self.sendMessageCallback = None
        self.getMessagesCallback = {}
        self.shutdown_event = threading.Event()
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
    
    def setGetMessagesCallback(self, tag, callback):
        self.getMessagesCallback[tag] = callback
    
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
        
    def getMessages(self, request, context):
        # Input: worker_type.GetMessagesRequest
        # Output: worker_type.GetMessagesResponse
        
        self.LOG.debug('receive getMessages request')
        
        tags = request.tags
        
        
        try:
            
            results = []
        
            for tag in tags:
                if tag in self.getMessagesCallback:
                    self.LOG.debug('call getMessagesCallback[{}]'.format(tag))
                    result = self.getMessagesCallback[tag](request, context)
                    
                    results.append(str(result))
                    
            status = self._getStatusObject('OK')
            response = worker_type.GetMessagesResponse(results=results, status=status)
        except Exception as e:
            status, errcode = self._handleError('getMessages', e)
            response = worker_type.GetMessagesResponse(status=status, error=errcode)
            
        return response
        
    def shutdown(self, request, context):
    
        self.LOG.debug('receive shutdown request')
        
        try:
            status = self._getStatusObject('OK')
            response = common_type.StatusResponse(status=status)
            
            self.LOG.debug('send interrupt signal to main thread')
            
            
            _thread.interrupt_main() # send KeyboardInterrupt to main thread
            self.shutdown_event.set()
            
        
        except Exception as e:
            status, errcode = self._handleError('shutdown', e)
            response = common_type.StatusResponse(status=status, error=errcode)
            
        return response
    
    def getWelcomeMessage(self, request, context):
        # Input: common_type.Token
        # Output: common_type.StatusResponse

        self.LOG.debug('receive getWelcomeMessage request')

        try:
            
            status = self._getStatusObject('OK')

            response = common_type.StatusResponse(result=self.welcome_message, status=status)

        except Exception as e:
            status, errcode = self._handleError('getWelcomeMessage', e)
            response = common_type.StatusResponse(status=status, error=errcode)

        return response

class DefaultWorkerServer(object):
    def __init__(self, max_workers=2):
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
        
        self.servicer = DefaultWorkerServicer()
        
        pb2_grpc.add_WorkerServicer_to_server(self.servicer, self.server)
        
    def start(self, port, block=False):
        self.server.add_insecure_port('[::]:{}'.format(port))
        
        self.server.start()
        print('server listening at port: {}'.format(port))
        
        try:
            if block:
                self.servicer.shutdown_event.wait()
        
        except KeyboardInterrupt as e:
            self.stop()
    
    def block(self):
        self.servicer.shutdown_event.wait()
    
    def stop(self):
        #print('stop server')
        time.sleep(1)
        self.server.stop(0)
