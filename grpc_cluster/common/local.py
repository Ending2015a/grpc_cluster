import os
import subprocess
import shlex
import time
import traceback
import hashlib
import datetime
import logging

class LocalServicer:
    def __init__(self, logger):
    
        self.LOG = logger
        self.__handles = {}
        self.__process_number = 0
    
    def _appendProcess(self, p):
        self.__process_number += 1
        self.__handles[self.__process_number] = p
        return self.__process_number
    
    def exeCommand(self, command, env=None, timeout=None, wait=False, print_output=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE):
        # execute command and return hashed process handle
        
        command_list = [os.path.expanduser(x) for x in shlex.split(command)]
        
        self.LOG.debug('parse command: {}'.format(command_list))
        
        p = subprocess.Popen(command_list, env=env, stdout=stdout, stderr=stderr)
        
        output = ''
        while wait:
            o = p.stdout.readline().decode('utf-8')
            if o == '' and p.poll() is not None:
                return p.poll(), output, None  # return code, output, process=None
            
            if output == '':
                output = o
            else:
                output = output + '\n' + o
            
            if print_output:
                self.LOG.info('{}'.format(o))
            
        process_number = self._appendProcess(p)
        
        return None, output, process_number
        
    def forceStopProcess(self, process_number):
        self.LOG.debug('call forceStopProcess: {}'.format(process_number))
        self.__handles[process_number].kill()
        del self.__handles[process_number]
    
    
    def forceStopAllProcess(self):
        self.LOG.debug('call forceStopAllProcess: {}'.format(process_number))
        for process_number in self.__handles:
            self.__handles[process_number].kill()
            del self.__handles[process_number]
            
        
