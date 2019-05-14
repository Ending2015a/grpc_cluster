
if __name__ != "__main__":
    try:
        __import__('pkg_resources').declare_namespace(__name__)
        
    except ImportError:
        __path__ = __import__('pkgutil').extend_path(__path__, __name__)
        
    from .master_server import DefaultMasterServer
    from .master_server import DefaultMasterServicer
    from .master_client import DefaultMasterClient
    
    __all__ = ['DefaultMasterServer', 'DefaultMasterServicer', 'DefaultMasterClient']
