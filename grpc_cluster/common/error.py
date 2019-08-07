import os
import sys
import time
import logging

class errno:
    

class gRPCClusterError(Exception):



    def __init__(self, message, error_type, errno=0):
        super().__init__(message)
        self.type = error_type

class ConnError(gRPCClusterError):
    def __init__(self, message='connection timeout'):
        super().__init__(message, type(self).__name__, errno=)
