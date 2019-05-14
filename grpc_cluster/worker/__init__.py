
if __name__ != "__main__":
    try:
        __import__('pkg_resources').declare_namespace(__name__)
        
    except ImportError:
        __path__ = __import__('pkgutil').extend_path(__path__, __name__)
        
    from .worker_server import DefaultWorkerServer
    from .worker_client import DefaultWorkerClient
    from . import worker_type_pb2 as worker_type
    
    __all__ = ['DefaultWorkerServer', 'DefaultWorkerClient', 'worker_type']
