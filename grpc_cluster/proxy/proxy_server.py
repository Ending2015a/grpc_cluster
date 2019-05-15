import os
import subprocess
import shlex
import time
import traceback
import hashlib
import datetime
from concurrent import futures

## any type
from google.protobuf import any_pb2
from google.protobuf import empty_pb2

## message types
from grpc_cluster.common import common_type
from grpc_cluster.common import ExceptionWrapper as Ewrapper

## proxy server
import grpc
from grpc_cluster.proxy import proxy_pb2_grpc as pb2_grpc
from grpc_cluster.proxy import proxy_pb2 as pb2

## logging
import logging
from grpc_cluster.logger import *

from grpc_cluster.common.auth import AuthenticationServicer

CHUNK_SIZE= 1024 * 1024  # 1024 * 1024 bytes = 1MB
UPLOAD_KEY_LENGTH = 8    # bytes
ADD_ADMIN_USER=True
ADMIN_USERNAME='admin'
ADMIN_PASSWORD='admin'
ALLOW_GUESTS_REGISTER=False
RESET_USER_DATA=False
DEFAULT_TOKEN_PATH='token.json'
DEFAULT_USER_PATH='user.json'



class DefaultProxyServicer(pb2_grpc.ProxyServicer, AuthenticationServicer):


    def __init__(self, token_file_path=DEFAULT_TOKEN_PATH,
                        user_file_path=DEFAULT_USER_PATH,
                        add_admin_user=ADD_ADMIN_USER,
                        admin_username=ADMIN_USERNAME,
                        admin_password=ADMIN_PASSWORD,
                        allow_guests_register=ALLOW_GUESTS_REGISTER,
                        reset_user_data=RESET_USER_DATA,
                        logger_name='DefaultProxyServicer',
                        logger_level='DEBUG'):

        self.add_admin_user = add_admin_user
        self.admin_username = admin_username
        self.admin_password = admin_password
        self.allow_guests_register = allow_guests_register
        self.reset_user_data = reset_user_data
        self.welcome_message = 'Heeeelllooooo~~~~ <3'
        self.logger_name = logger_name
        self.logger_level= logger_level


        # --- initialize

        # initialize authentication service
        AuthenticationServicer.__init__(self=self,
                                        token_file_path=token_file_path,
                                        user_file_path=user_file_path,
                                        clear=self.reset_user_data,
                                        logger_level=logger_level)

        self._worker_map = {}
        self._upload_key_map = {} # store upload file keys
        self._initializeLogger(logger_name, logger_level)

        # create admin user
        if self.add_admin_user:
            if not self.validateUser(self.admin_username):
                self.registerNewUser(self.admin_username, self.admin_password)

    def _initializeLogger(self, logger_name, logger_level):
        # 
        # initialize logger
        loadConfig()
        self.LOG = createLoggerFromExistedLogger(logger_name, logger_level)

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

    def _getFileInfoFromUploadKeyMap(self, key):
        #
        # get fileinfo

        try:
            fileinfo = self._upload_key_map[key.value]
        except:
            raise KeyError('upload_key_map does not contain key: {}'.format(key.value))
        return fileinfo

    def _validateKey(self, upload_key):
        return upload_key.value in self._upload_key_map

    def _generateKey(self, token, filename):
        #
        # use token and filename to generate upload key value
        #    the key value using blake2b hash has length=8 (default)
        # return comon_type.Key

        value = hashlib.blake2b(token.value, digest_size=UPLOAD_KEY_LENGTH)
        value.update(filename.encode('utf-8'))

        key = common_type.Key(value = value.digest())

        # regenerate until no conflict
        while key.value in self._upload_key_map:
            value.update(key.value)

            key.value = value.digest()

        return key

    def _updateKeyMap(self, key, fileinfo, owner):
        self.LOG.debug('the key {} has been updated: filename={}, size={}, owner={}'.format(key, fileinfo.name, fileinfo.size, owner))

        self._upload_key_map[key.value] = {
                'name': fileinfo.name,
                'size': fileinfo.size,
                'fp': None,
                'owner': owner
                }

    def _getStatusObject(self, status='OK'):
        # 'UNKNOWN' / 'OK' / 'FAILED'
        assert status == 'OK' or status == 'UNKNOWN' or status == 'FAILED'
        return common_type.Status.Value(status)


    # --- rpc

    def uploadFileInfo(self, request, context):
        # Input: common_type.UploadFileInfoRequest
        # Output: common_type.UploadFileInfoResponse

        token = request.token
        fileinfo = request.info

        self.LOG.debug('receive uploadFileInfo request')
        self.LOG.debug('    token={}, filename={}, size={}'.format(token, fileinfo.name, fileinfo.size))

        try:
            self.authenticateToken(token) # throw Exception

            owner = self.getUsernameByToken(token)

            key = self._generateKey(token, fileinfo.name)
            self._updateKeyMap(key, fileinfo, owner)
            
            status = self._getStatusObject('OK')
            response = common_type.UploadFileInfoResponse(key=key, status=status)
        except Exception as e:
            status, errcode = self._handleError('uploadFileInfo', e)
            response = common_type.UploadFileInfoResponse(key=key, status=status, error=errcode)

        return response


    def uploadFile(self, request_iterator, context):
        # Input: stream common_type.UploadFileRequest
        # Output: common_type.StatusResponse

        self.LOG.debug('receive uploadFile request')

        try:
            for request_chunk in request_iterator:
                chunk = request_chunk.chunk

                key = chunk.key
            
                fileinfo = self._getFileInfoFromUploadKeyMap(key) #throw key error
                filename = fileinfo['name']
                
                filepath = os.path.dirname(fileinfo['name'])
                self.LOG.debug('upload filename: {} / filepath: {}'.format(fileinfo['name'], filepath))
                
                if not os.path.exists(filepath) and filepath != '':
                    os.makedirs(filepath)

                if fileinfo['fp'] == None:
                    fileinfo['fp'] = open(filename, 'wb')

                fileinfo['fp'].write(chunk.buffer)

            fileinfo['fp'].close()

            if os.path.getsize(fileinfo['name']) != fileinfo['size']:
                raise Exception('size of the uploaded file does not match to FileInfo.size uploaded by owner: {}'.format(
                                                                                    fileinfo['owner']))

            status = self._getStatusObject('OK')
            response = common_type.StatusResponse(result='upload file successfully', status=status)

        except Exception as e:
            status, errcode = self._handleError('uploadFile', e)
            response = common_type.StatusResponse(result='failed to upload file', status=status, error=errcode)

        return response

    def downloadFile(self, request, context):
        # Input: common_type.DownloadFileRequest
        # Output: Stream DownloadFileResponse

        self.LOG.debug('receive downloadFile request')

        token = request.token
        fileinfo = request.info

        def __errhandler(scope, e):
            self._reportError('exception occurred')

        try:
            self.authenticateToken(token) # throw exception


            def download_iter(filename):
                try:
                    status = self._getStatusObject('OK')
                    with open(filename, 'rb') as f:
                        while True:
                            piece = f.read(CHUNK_SIZE)
                            if len(piece) == 0:
                                return
                        
                            chunk = common_type.FileChunk(buffer=piece)
                        
                            yield common_type.DownloadFileResponse(chunk=chunk, status=status)
                except Exception as e:
                    status, errcode = self._handleError('download_iter', e)
                    return common_type.DownloadFileResponse(status=status, error=errcode)
            
            return download_iter(fileinfo.name)

        except Exception as e:
            status, errcode = self._handleError('downloadFile', e)
            return common_type.DownloadFileResponse(status=status, error=errcode)
        

    def getDirectoryList(self, request, context):
        #TODO:
        # return directory list
        # Input: common_type.GetDirectoryListRequest
        # Output: common_type.StatusPresponse
        try:
            raise NotImplementedError('getDirectoryList not implemented')

        except Exception as e:
            status, errcode = self._handleError('getDirectoryList', e)
            response = common_type.StatusResponse(status=status, error=errcode)
            return reponse

    def getFileInfo(self, request, context):
        #TODO:
        # return file information
        # Input: common_type.GetFileInfoRequest
        # Output: common_type.FileInfoResponse
        try:
            raise NotImplementedError('getFileInfo not implemented')

        except Exception as e:
            status, errcode = self._handleError('getFileInfo', e)
            response = common_type.StatusResponse(status=status, error=errcode)
            return response

    def moveFileOrDirectory(self, request, context):
        #TODO:
        # move file or directory
        # Input: common_type.MoveFileOrDirectoryRequest
        # Output: common_type.StatusResponse
        try:
            raise NotImplementedError('moveFileOrDirectory not implemented')
        
        except Exception as e:
            status, errcode = self._handleError('moveFileOrDirectory', e)
            response = common_type.StatusResponse(status=status, error=errcode)
            return response

    def removeFileOrDirectory(self, request, context):
        #TODO:
        # remove file or directory
        # Input: common_type.RemoveFileOrDirectoryRequest
        # Output: common_type.StatusResponse
        try:
            raise NotImplementedError('removeFileOrDirectory not implemented')

        except Exception as e:
            status, errcode = self._handleError('removeFileOrDirectory', e)
            response = common_type.StatusResponse(status=status, error=errcode)
            return response

    def executeCommand(self, request, context):
        #TODO:
        # execute command, capture stdout and return excuted results
        # Input: common_type.ExecuteCommandRequest
        # output: common_type.StatusResponse
        self.LOG.debug('receive executeCommand')
        token = request.token
        command = request.command.value
        timeout = request.command.timeout

        if not timeout > 0:
            timeout = None

        try:
            self.authenticateToken(token) #throw exception

            self.LOG.warning('execute command: {}'.format(command))

            # parsing command
            cmd_list = shlex.split(command)

            self.LOG.debug('parse command: {}'.format(cmd_list))

            # execute command
            raw_output = subprocess.check_output(cmd_list, timeout=timeout)
            output = raw_output.decode('utf-8')
            
            self.LOG.debug('get command output: {}'.format(output))

            status = self._getStatusObject('OK')
            
            self.LOG.debug('get command output: {}'.format(output))

            response = common_type.StatusResponse(result=output, status=status)

        except subprocess.CalledProcessError as e:
            output = e.output.decode('utf-8')   # byte string -> decode -> string
            code = e.returncode                 # int
            status, errcode = self._handleError('executeCommand while executing user command', e)
            response = common_type.StatusResponse(result=output, status=status, error=errcode)

        except subprocess.TimeoutExpired as e:
            output = e.output.decode('utf-8')   # byte string -> decode -> string
            status, errcode = self._handleError('executeCommand while executing user command', e)
            response = common_type.StatusResponse(result=output, status=status, error=errcode)

        except Exception as e:
            status, errcode = self._handleError('executeCommand', e)
            response = common_type.StatusResponse(status=status, error=errcode)
            
        return response


    def register(self, request, context):
        # Input: common_type.RegisterRequest
        # Output: common_type.TokenResponse

        self.LOG.debug('receive register request')

        token = request.token
        username = request.username
        password = request.password

        try:
            if not self.allow_guests_register:
                self.authenticateToken(token) #throw exception
            
            token = self.registerNewUser(username, password)

            status = self._getStatusObject('OK')

            response = common_type.TokenResponse(token=token, status=status)

        except Exception as e:
            status, errcode = self._handleError('register', e)
            response = common_type.TokenResponse(status=status, error=errcode)

        return response
        

    def signIn(self, request, context):
        # Input: common_type.signInRequest
        # Output: common_type.TokenResponse

        self.LOG.debug('receive signIn request')

        username = request.username
        password = request.password


        try:
            token = self.loginUser(username, password)

            status = self._getStatusObject('OK')
            response = common_type.TokenResponse(token=token, status=status)

        except Exception as e:
            status, errcode = self._handleError('signIn', e)
            response = common_type.TokenResponse(status=status, error=errcode)

        return response


    def checkUserExistance(self, request, context):
        # Input: common_type.CheckUserExistanceRequest
        # Output: common_type.StatusResponse
        
        self.LOG.debug('receive checkUserExistance request')

        username = request.username

        try:
            
            result = self.validateUser(username)

            status = self._getStatusObject('OK')
            response = common_type.StatusResponse(result=str(result), status=status)

        except Exception as e:
            status, errcode = self._handleError('chechUserExistance', e)
            response = common_type.StatusResponse(status=status, error=errcode)

        return response

    def sendMessage(self, request, context):
        # Input: common_type.SendMessageRequest
        # Output: common_type.StatusResponse

        self.LOG.debug('receive sendMessage request')

        token = request.token
        msg = request.msg

        try:
            self.authenticateToken(token) #throw exception

            username = self.getUsernameByToken(token)

            self.LOG.info('[user {}]: {}'.format(username, msg))

            status = self._getStatusObject('OK')
            response = common_type.StatusResponse(status=status)

        except Exception as e:
            status, errcode = self._handleError('sendMessage', e)
            response = common_type.StatusResponse(status=status, error=errcode)
        
        return response


    def getWelcomeMessage(self, request, context):
        # Input: empty_pb2.Empty
        # Output: common_type.StatusResponse

        self.LOG.debug('receive getWelComeMessage request')

        try:
            
            status = self._getStatusObject('OK')

            response = common_type.StatusResponse(result=self.welcome_message, status=status)

        except Exception as e:
            status, errcode = self._handleError('getWelcomeMessage', e)
            response = common_type.StatusResponse(status=status, error=errcode)

        return response
        
    def launchWorkers(self, request, context):
        # Input: common_type.LaunchWorkerRequest
        # Output: common_type.StatusResponse
        
        self.LOG.debug('receive launchWorkers request')
        
        token = request.token
        config = request.configuration
        
        try:
            self.authenticateToken(token)
            
            username = self.getUsernameByToken(token)
            
            master_name = config.master.name
            master_addr = config.master.addr
            
            proxy_config = config.proxy
            
            proxy_name = proxy_config.name
            
            
            default_entrypoint = proxy_config.default_entrypoint
            default_venv = proxy_config.default_venv
            if default_venv != '':
                default_venv = os.path.expanduser(default_venv)
            
            worker_configs = proxy_config.workers
            

            if len(worker_configs) == 0:
                raise Exception('no worker definition found')
 
            for worker in worker_configs:
                worker_name = worker.name
                worker_fullname = '{}/{}'.format(proxy_name, worker.name)
                worker_port = worker.port
                worker_entrypoint = default_entrypoint if worker.entrypoint == '' else worker.entrypoint
                worker_venv = default_venv if worker.venv == '' else worker.venv
                worker_envs = {}

                if worker_name == '':
                    raise Exception('no name specified for worker')

                if worker_port == '':
                    raise Exception('no port number specified for worker={}'.format(name))                

                if len(worker.environment) > 0:
                    for env in worker.environment:
                        worker_envs[env.variable] = env.value
                
                if worker_entrypoint == '':
                    raise Exception('no entrypoint specified for worker={}, port={}'.format(worker_fullname, worker_port))
                
                worker_entrypoint = os.path.abspath(worker_entrypoint)
                
                if worker_venv == '':
                    python_path = 'python3'
                    lib_path = ''
                else:
                    worker_venv = os.path.expanduser(worker_venv)
                    python_path = os.path.abspath('{}/bin/python'.format(worker_venv))    
                    lib_path = os.path.abspath('{}/lib'.format(worker_venv))
                
                def _get_grpc_cluster_path():
                    import grpc_cluster
                    fullpath = os.path.abspath(grpc_cluster.__file__)
                    return os.path.dirname(os.path.dirname(fullpath))


                def _launch_single_worker(worker_name, worker_fullname, worker_port):
                    # launch worker
                    env = os.environ.copy()
                    env['GRPC_CLUSTER_ROOT'] = _get_grpc_cluster_path()
                    env['CLUSTER_ROOT'] = str(os.path.abspath('./'))
                    env['CLUSTER_WORKER_ROOT'] = str(os.path.join(env['CLUSTER_ROOT'], worker_name))
                    env['CLUSTER_WORKER_NAME'] = str(worker_fullname)
                    env['CLUSTER_WORKER_PORT'] = str(worker_port)
                    env['CLUSTER_MASTER_NAME'] = str(master_name)
                    env['CLUSTER_MASTER_ADDR'] = str(master_addr)
                    # add venv library path, cluster root path
                    env['PATH'] = '{}:{}:'.format(lib_path, env['CLUSTER_ROOT']) + env['PATH']
                
                    for e in worker_envs:
                        env[e] = str(worker_envs[e])
                    
                    command = '{} \"{}\"'.format(python_path, worker_entrypoint)
                
                    cmds = shlex.split(command)
                
                    current_time = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
                    output_file_name = '{}_{}_{}'.format(worker_name, worker_port, current_time)
                    
                    stdout_name = '{}.out'.format(output_file_name)
                    stderr_name = '{}.err'.format(output_file_name)
                
                
                    self.LOG.debug('    start worker:')
                    self.LOG.debug('        name: {}'.format(worker_name))
                    self.LOG.debug('    fullname: {}'.format(worker_fullname))
                    self.LOG.debug('        port: {}'.format(worker_port))
                    self.LOG.debug('  entrypoint: {}'.format(worker_entrypoint))
                    self.LOG.debug('        venv: {}'.format(worker_venv))
                    self.LOG.debug('        envs: {}'.format(worker_envs))
                    self.LOG.debug('      stdout: {}'.format(stdout_name))
                    self.LOG.debug('      stderr: {}'.format(stdout_name))
                    self.LOG.debug('       owner: {}'.format(username))
                
                
                    # start worker
                    worker_stdout = open(stdout_name, "wb")
                    # worker_stderr = open(stderr_name, "wb")
                    process = subprocess.Popen(cmds, env=env, stdout=worker_stdout, stderr=worker_stdout)
                
                    self._worker_map[worker_name] = {
                        'name': worker_name,
                        'fullname': worker_fullname,
                        'port': worker_port,
                        'entrypoint': worker_entrypoint,
                        'venv': worker_venv,
                        'envs': worker_envs,
                        'process': process,
                        'stdout': worker_stdout,
                        #'stderr': worker_stderr,
                        'owner': username,
                        }
            
                if '-' in worker_port:
                    start_port, end_port = worker_port.split('-')
                    start_port = int(start_port.strip())
                    end_port = int(end_port.strip())
                    launch_port = [x for x in range(start_port, end_port+1)]

                    for port in launch_port:
                        _worker_name = '{}_{}'.format(worker_name, port)
                        _worker_fullname = '{}_{}'.format(worker_fullname, port)
                        _worker_port = port
                        _launch_single_worker(_worker_name, _worker_fullname, _worker_port)

                else:
                    _launch_single_worker(worker_name, worker_fullname, worker_port)

            
            status = self._getStatusObject('OK')
            response = common_type.StatusResponse(status=status)

        except Exception as e:
            status, errcode = self._handleError('launchWorker', e)
            response = common_type.StatusResponse(status=status, error=errcode)

        return response
                
    
    def createVirtualenv(self, request, context):
        # Input: common_type.CreateVirtualenvRequest
        # Output: common_type.StatusResponse
        
        self.LOG.debug('receive createVirtualenv request')
        
        token = request.token
        config = request.configuration
        
        env_path = config.path
        params = config.params
        requirements = config.requirements
        
        output = ''
        
        try:
            self.authenticateToken(token) #throw exception
            
            
            def exeCommand(command, env=None):
                cmds = shlex.split(command)
                self.LOG.info('execute command: {}'.format(cmds))
                
                # execute command with specified env
                p = subprocess.Popen(cmds, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                
                output = ''
                
                while True:
                    o = p.stdout.readline().decode('utf-8')
                    if o == '' and p.poll() is not None:
                        break
                    
                    output = output + '\n' + o
                    self.LOG.info('  {}'.format(o))
                    
                rc = p.poll()
                self.LOG.info('  exit code: {}'.format(rc))
                
                if rc != 0:
                    raise subprocess.CalledProcessError(returncode=rc,
                                                        cmd=cmds,
                                                        output=output)
                
                return rc, output
                

            self.LOG.info('creating virtualenv: {}'.format(env_path))
            
            param_string = ''
            if len(params) > 0:
                for _param in params:
                    _param = _param.replace(' ', '')
                    _param = "--{}".format(_param)
                    param_string = param_string + _param
                
                    self.LOG.info('    add param: {}'.format(_param))
            

            rc, o = exeCommand('virtualenv {} {}'.format(param_string, os.path.expanduser(env_path)))
            output = output + '\n' + o
            
            self.LOG.info('copying env')
            env = os.environ.copy()
            lib_path = os.path.abspath('{}/lib'.format(env_path))
            env["PATH"] = '{}:'.format(lib_path) + env["PATH"]
            self.LOG.info('  add PATH to env: {}'.format(lib_path))
            
            self.LOG.info('installing dependencies')
            python_path = os.path.abspath('{}/bin/python'.format(env_path))
            pip_path = os.path.abspath('{}/bin/pip'.format(env_path))
            for lib in requirements:
                self.LOG.info('installing dependencies: {}'.format(lib))
                rc, o = exeCommand('{} install {}'.format(pip_path, lib))
                output = output + '\n' + o
                
                
            status = self._getStatusObject('OK')
            response = common_type.StatusResponse(result=output, status=status)
            
        except subprocess.CalledProcessError as e:
            output = e.output   
            code = e.returncode                 # int
            status, errcode = self._handleError('createVirtualenv while doing some stuff', e)
            response = common_type.StatusResponse(result=output, status=status, error=errcode)
            
        except Exception as e:
            status, errcode = self._handleError('createVirtualenv', e)
            response = common_type.StatusResponse(result=output, status=status, error=errcode)
                
        return response
    
    def getPipe(self, request, context):
        
        token = request.token
        worker_name = request.name
        
        try:
            self.authenticateToken(token)
            
            username = self.getUsernameByToken(token)
        
            def pipe_iter(self, worker_name):
            
                
                process = self._worker_map[worker_name]
                
                while True:
                    output = process.stdout.readline().decode('utf-8')
                    if output == '' and p.poll() is not None:
                        break
                    error = process.stderr.readline().decode('utf-8')
                    yield common_type.PipeResponse(stdout=output, stderr=error, exit=False)
                    
                rc = p.poll()
                
                
                return common_type.PipeResponse(stdout=output, stderr=error, exit=True, exit_code=rc)
                
            
            return pipe_iter(self, worker_name)
            
        except Exception as e:
            status, errcode = self._handleError('getPipe', e)
            response = common_type.PipeResponse(status=status, error=errcode)
            return respones
       
    def __del__(self):
        try:
            for worker_name in self._worker_map:
                try:
                    worker = self._worker_map[worker_name]
                    worker['process'].kill()
                    worker['stdout'].close()
                    worker['stderr'].close()
                except:
                    pass
                    
                self.LOG.info('worker {} has beed stopped'.format(worker['name']))
        except:
            pass

'''
class ProxyServicer(DefaultProxyServicer):
    def __init__(self, token_file_path,
                        user_file_path,
                        add_admin_user=ADD_ADMIN_USER,
                        admin_username=ADMIN_USERNAME,
                        admin_password=ADMIN_PASSWORD,
                        allow_guests_register=ALLOW_GUESTS_REGISTER,
                        reset_user_data=RESET_USER_DATA,
                        welcome_message='hello',
                        logger_name='LowEndProxyServicer',
                        logger_level='DEBUG'):

        super(ProxyServicer, self).__init__(token_file_path,
                                            user_file_path,
                                            add_admin_user,
                                            admin_username,
                                            admin_password,
                                            allow_guests_register,
                                            reset_user_data,
                                            welcome_message,
                                            logger_name,
                                            logger_level)

        self._API_CALLBACK = {}

    def registerAPI(self, name, func):
    
        self._API_CALLBACK[name] = func
        

    def uploadFileInfo(self, request, context):
'''     
        

# wrapper
class DefaultProxyServer(object):
    
    def __init__(self, max_workers=5,
                        token_file_path=DEFAULT_TOKEN_PATH,
                        user_file_path=DEFAULT_USER_PATH,
                        add_admin_user=ADD_ADMIN_USER,
                        admin_username=ADMIN_USERNAME,
                        admin_password=ADMIN_PASSWORD,
                        allow_guests_register=ALLOW_GUESTS_REGISTER,
                        reset_user_data=RESET_USER_DATA):
    
        self.server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
        
        self.servicer = DefaultProxyServicer(
                        token_file_path=token_file_path,
                        user_file_path=user_file_path,
                        add_admin_user=add_admin_user,
                        admin_username=admin_username,
                        admin_password=admin_password,
                        allow_guests_register=allow_guests_register,
                        reset_user_data=reset_user_data)
        
        pb2_grpc.add_ProxyServicer_to_server(self.servicer, self.server)
        
    def start(self, port):
        self.server.add_insecure_port('[::]:{}'.format(port))
        
        self.server.start()
        print('server listening at port: {}'.format(port))
        
        try:
            while True:
                time.sleep(60*60*24)
                
        except KeyboardInterrupt:
            self.server.stop(0)
            




