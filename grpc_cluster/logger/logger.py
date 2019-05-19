import logging
import logging.config
import datetime
import os
import errno

filename = 'logger.conf'
logfile_prefix = 'log/'
logfile_suffix = ''



def makedir(path):
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def getLogTimeString(time_format='%Y%m%d_%H%M%S'):
    return datetime.datetime.now().strftime(time_format)

def loadConfig(filename=filename):
    if "loaded" not in loadConfig.__dict__:
        loadConfig.loaded = False
    if not loadConfig.loaded:
        makedir(os.path.abspath(logfile_prefix))
        filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
        logtime = getLogTimeString()
        logging.config.fileConfig(filepath, defaults={
                                        'prefix': logfile_prefix,
                                        'logtime': getLogTimeString(),
                                        'suffix': logfile_suffix}, 
                                    disable_existing_loggers=False)
        loadConfig.loaded = True

def checkLoggerExist(logger_name):
    return logger_name in logging.Logger.manager.loggerDict


def createLoggerFromExistedLogger(logger_name, existed_logger_name=None, log_to_file=True):
    if (not existed_logger_name == None ) and not checkLoggerExist(existed_logger_name):
        logging.getLogger().warning('logger: {} does not exist'.format(existed_logger_name))
        return logging.getLogger(logger_name)

    if checkLoggerExist(logger_name):
        logging.getLogger().warning('logger already existed: {}'.format(logger_name))

    ROOT = logging.getLogger()
    EX = logging.getLogger(existed_logger_name)
    LOG = logging.getLogger(logger_name)

    LOG.addHandler(ROOT.handlers[0])
    if log_to_file:
        LOG.addHandler(ROOT.handlers[1])
    LOG.setLevel(EX.getEffectiveLevel())
    LOG.propagate = False
    #for handler in EX.handlers:
    #    LOG.addHandler(handler)

    return LOG
