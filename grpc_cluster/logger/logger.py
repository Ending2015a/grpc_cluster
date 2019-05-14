import logging
import logging.config
import datetime
import os

filename = 'logger.conf'
logfile_prefix = 'log/'
logfile_suffix = ''



def makedir(path):
    try:
        os.makedirs(path)
    except OSError:
        pass

def loadConfig(filename=filename):
    makedir(os.path.abspath(logfile_prefix))
    filepath = os.path.join(os.path.dirname(os.path.abspath(__file__)), filename)
    logtime = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    logging.config.fileConfig(filepath, defaults={
                                        'prefix': logfile_prefix,
                                        'logtime': logtime,
                                        'suffix': logfile_suffix}, 
                                    disable_existing_loggers=False)

def checkLoggerExist(logger_name):
    return logger_name in logging.Logger.manager.loggerDict

def createLoggerFromExistedLogger(logger_name, existed_logger_name=None):
    if (not existed_logger_name == None ) and not checkLoggerExist(existed_logger_name):
        logging.getLogger().warning('logger: {} does not exist'.format(existed_logger_name))
        return logging.getLogger(logger_name)

    if checkLoggerExist(logger_name):
        logging.getLogger().warning('logger already existed: {}'.format(logger_name))
    
    EX = logging.getLogger(existed_logger_name)
    LOG = logging.getLogger(logger_name)

    LOG.setLevel(EX.getEffectiveLevel())
    #for handler in EX.handlers:
    #    LOG.addHandler(handler)

    return LOG
