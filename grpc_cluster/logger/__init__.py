
try:
    from grpc_cluster.logger.logger import loadConfig
    from grpc_cluster.logger.logger import checkLoggerExist
    from grpc_cluster.logger.logger import createLoggerFromExistedLogger

except  ModuleNotFoundError:
    from .logger import loadConfig
    from .logger import checkLoggerExist
    from .logger import createLoggerFromExistedLogger


__all__ = ['loadConfig', 'checkLoggerExist', 'createLoggerFromExistedLogger']
