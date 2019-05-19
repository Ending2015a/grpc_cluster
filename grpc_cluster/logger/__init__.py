
try:
    from grpc_cluster.logger.logger import loadConfig
    from grpc_cluster.logger.logger import checkLoggerExist
    from grpc_cluster.logger.logger import createLoggerFromExistedLogger
    from grpc_cluster.logger.logger import getLogTimeString

except  ModuleNotFoundError:
    from .logger import loadConfig
    from .logger import checkLoggerExist
    from .logger import createLoggerFromExistedLogger
    from .logger import getLogTimeString


__all__ = ['loadConfig', 'checkLoggerExist', 'createLoggerFromExistedLogger', 'getLogTimeString']
