from grpc_tools import protoc
import subprocess
import os
import logger
import logging

LOG = None
#LEVEL = logging.DEBUG

def makeDirectories(path, log=False):
    
    try:
        if log:
            LOG.info('creating directories:')
            LOG.info('    {}'.format(path))

        # create nested directories
        os.makedirs(path)


    except OSError:
        pass

'''
def createLogger():
    logger = logging.getLogger(__file__)
    assert id(logging.getLogger(__file__)) == id(logging.getLogger(__file__))

    logger.setLevel(logging.DEBUG)


    makeDirectories(os.path.abspath('log'))

    fh = logging.FileHandler('log/{}.log'.format(__file__))
    fh.setLevel(logging.DEBUG)
    sh = logging.StreamHandler()
    sh.setLevel(LEVEL)
    formatter = logging.Formatter(
            '[%(asctime)s|%(threadName)s|%(levelname)s:%(name)s]: %(message)s',
            '%Y-%m-%d %H:%M:%S')

    sh.setFormatter(formatter)
    fh.setFormatter(formatter)

    logger.addHandler(sh)
    logger.addHandler(fh)
'''

def findIncludePath():
    # find protoc path
    # /.../bin/protoc
    x = subprocess.check_output(['which', 'protoc']).decode('utf-8')

    # walking...
    # /.../bin
    _bin = os.path.dirname(x)

    # walking...
    # /.../
    _root = os.path.dirname(_bin)

    # walking...
    # /.../include
    _inc = os.path.join(_root, 'include')

    # print log
    LOG.info('Found include path: {}'.format(_inc))
    
    return _inc

def gencode(proto, gen_path, include_paths):
    _incs = []

    # append -I to each path
    for inc in include_paths:
        # print log
        LOG.info('    add include path: {}'.format(inc))

        _incs.append('-I{}'.format(inc))

    # print log
    LOG.info('generating code to path: ')
    LOG.info('    {}'.format(os.path.join(
                os.path.abspath(gen_path),
                os.path.dirname(proto))))

    # certae directories
    makeDirectories(os.path.abspath(gen_path), log=True)

    # compile

    LOG.info('compiling proto file:')
    LOG.info('    {}'.format(proto))
    protoc.main((
        '',
        '-I.',
        *_incs,
        '--python_out={}'.format(gen_path),
        '--grpc_python_out={}'.format(gen_path),
        '{}'.format(proto)
        ))


if __name__ == '__main__':

    logger.loadConfig()
    LOG = logger.createLoggerFromExistedLogger('run_codegen', 'INFO')
    proto_root = './proto'

    include_paths = [
            findIncludePath(),
            '{}/grpc_cluster/rpc/common'.format(proto_root),
            '{}/grpc_cluster/rpc/proxy'.format(proto_root),
            '{}/grpc_cluster/rpc/master'.format(proto_root),
            '{}/grpc_cluster/rpc/worker'.format(proto_root),
            '{}/grpc_cluster/rpc'.format(proto_root),
            '{}'.format(proto_root)]

    # service
    # path = os.path.join(gen_path, proto)
    gencode(proto='proxy/proxy.proto',
            gen_path='./rpc/',
            include_paths=include_paths)
    gencode(proto='master/master.proto',
            gen_path='./rpc/',
            include_paths=include_paths)
    gencode(proto='worker/worker.proto',
            gen_path='./rpc/',
            include_paths=include_paths)
            
    # message
    gencode(proto='worker/worker_type.proto',
            gen_path='./rpc/',
            include_paths=include_paths)
    gencode(proto='common/common_type.proto',
            gen_path='./rpc/',
            include_paths=include_paths)
    

