
__version__ = "0.0.1"

try:
    __import__('pkg_resources').declare_namespace(__name__)
except ImportError:
    __path__ = __import('pkgutil').extend_path(__path__, __name__)


#from .cluster import Cluster

#__all__ = ['Cluster']
