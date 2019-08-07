
if __name__ != "__main__":
    try:
        __import__('pkg_resources').declare_namespace(__name__)

    except ImportError:
        __path__ = __import__('pkgutil').extend_path(__path__, __name__)

    from .proxy_server import DefaultProxyServer
    from .proxy_client import DefaultProxyClient
    
    __all__ = ['DefaultProxyServer', 'DefaultProxyClient']
