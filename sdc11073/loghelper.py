import logging
import traceback


def ensureLogStream():
    """Method makes sure that the pysdc root Logger has a stream handler with the default format.
    :return: pysdc root logger
    """
    applog = logging.getLogger('sdc')
    for handler in applog.handlers:
        if isinstance(handler, logging.StreamHandler):
            return
    ch = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to ch
    ch.setFormatter(formatter)
    # add ch to logger
    applog.addHandler(ch)
    return applog


def reset_log_levels(root_logger_name='sdc'):
    for name in logging.Logger.manager.loggerDict:
        if name.startswith(root_logger_name):
            logging.getLogger(name).setLevel(logging.NOTSET)


def reset_handlers(root_logger_name='sdc'):
    for name in logging.Logger.manager.loggerDict:
        if name.startswith(root_logger_name):
            logger = logging.getLogger(name)
            for handler in logger.handlers:
                logger.removeHandler(handler)


def basic_logging_setup(root_logger_name='sdc', level=logging.INFO, log_file_name=None):
    logging.basicConfig(format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=level)
    reset_log_levels(root_logger_name)
    reset_handlers(root_logger_name)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    if log_file_name:
        file_handler = logging.handlers.RotatingFileHandler(log_file_name,
                                                            maxBytes=5000000,
                                                            backupCount=2)
        file_handler.setFormatter(formatter)



class LoggerAdapter(object):
    """
    This adapter wraps a standard logger and changes the interface in two ways:
     - it uses .format() method of strings for formatting (in contrast to logging.Logger, which uses % operator).
     - if any argument in *args or **kwargs is callable, it replaces the argument with the returned value of the called argument.
       This helps to reduce processing time if the called method is expensive and logger is not enabled for given log level.
    """

    def __init__(self, logger, prefix=None):
        self.logger = logger
        self.log_prefix = prefix or ''

    def _process(self, msg, args, kwargs):
        try:
            _msg = self.log_prefix + msg
        except TypeError:
            _msg = msg

        if len(args) == len(kwargs) == 0:
            return _msg

        if '%' in msg and not '{' in msg:
            # traditional log formatting
            return _msg%args

        resolvedArgs = []
        for a in args:
            resolvedArgs.append(a() if callable(a) else a)
        resolvedKwargs = {}
        for k, a in kwargs.items():
            resolvedKwargs[k] = a() if callable(a) else a
        
        try:   
            fullmsg = _msg.format(*resolvedArgs, **resolvedKwargs)
        except:
            print (traceback.format_exc())
            raise
        return fullmsg


    def debug(self, msg, *args, **kwargs):
        self.log(logging.DEBUG, msg, *args, **kwargs)


    def info(self, msg, *args, **kwargs):
        self.log(logging.INFO, msg, *args, **kwargs)


    def warning(self, msg, *args, **kwargs):
        self.log(logging.WARN, msg, *args, **kwargs)

    warn = warning
    
    
    def error(self, msg, *args, **kwargs):
        self.log(logging.ERROR, msg, *args, **kwargs)


    def exception(self, msg, *args, **kwargs):
        self.logger.error(self._process(msg, args, kwargs), exc_info=1)


    def critical(self, msg, *args, **kwargs):
        self.log(logging.CRITICAL, msg, *args, **kwargs)


    def log(self, level, msg, *args, **kwargs):
        """
        Delegate a log call to the underlying logger, after processing msg, args and kwargs
        """
        if self.isEnabledFor(level):
            self.logger.log(level, self._process(msg, args, kwargs))


    def isEnabledFor(self, level):
        """
        See if the underlying logger is enabled for the specified level.
        """
        return self.logger.isEnabledFor(level)


def getLoggerAdapter(name, prefix=None):
    ''' Use this method instead of logging.getLogger.
    @return: a LoggerAdapter instance
    '''
    return LoggerAdapter(logging.getLogger(name), prefix)


class LogWatchException(Exception):
    def __init__(self, issues):
        super(LogWatchException, self).__init__()
        self.issues = issues

    def __repr__(self):
        return 'LogWatchException: {}'.format(self.issues)


class _LogIssue(object):
    def __init__(self, record):
        self.record = record
        self.call_stack = traceback.format_stack(limit=15)
        # remove last lines from call stack that are inside logging and loghelper.
        # By doing this the call stack shows the call to the logger as last entry.
        while __file__ in self.call_stack[-1] or logging.__file__ in self.call_stack[-1]:
            del self.call_stack[-1]

    def __repr__(self):

        return 'log msg="{}" level={} thread="{}"; call-stack:\n{}'.format(self.record.msg,
                                                                    self.record.levelname,
                                                                    self.record.threadName or self.record.thread,
                                                                    ''.join(self.call_stack))

class LogWatcherHandler(logging.Handler):
    ''' This is a logging handler that stores all records in a list'''
    def __init__(self, logger, level):
        '''
        This is a logging handler that stores all records in a list.
        :param logger: the logger that shall be handled
        :param level: all records with log level >= level will be recorded
        '''
        super(LogWatcherHandler, self).__init__(level=level)
        self._logger = logger
        self.records = []
        self._logger.addHandler(self)

    def emit(self, record):
        '''
        This method is called by logger if record log level >= own level
        '''
        self.acquire()
        try:
            self.records.append(_LogIssue(record))
        finally:
            self.release()

    def disconnect(self):
        '''Remove self from logger.'''
        self._logger.removeHandler(self)

    def clear(self):
        ''' Delete all records'''
        self.acquire()
        try:
            del self.records[:]
        finally:
            self.release()


class LogWatcher(object):
    '''Manages one or more LogWatcherHandlers.
    Can be used also as contextmanager'''
    def __init__(self, logger, level=logging.ERROR, startPaused=False):
        '''
        :param logger: the initial logger that shall be recorded
        :param level:  the log level for the initial handler
        :param startPaused: if true, logging is not started immediately.
        '''
        self._logger = logger
        self._level = level
        self.handlers = []
        self._collecting = False
        self.addHandler(logger, level)
        self._collecting = not startPaused

    def addHandler(self, logger, level):
        '''
        Add another LogWatcherHandler.
        :param logger: the logger that shall be recorded
        :param level: the log level for the handler
        :return: a LogWatcherHandler instance
        '''
        coll = LogWatcherHandler(logger, level)
        coll.addFilter(self)
        self.handlers.append(coll)
        return coll

    def setPaused(self, isPaused):
        '''
        Enable/disable recording.
        :param isPaused: if True, no records will be saved.
        :return:
        '''
        self._collecting = not isPaused

    def stop(self):
        '''
        Disconnect and delete all Handlers
        :return:
        '''
        self._collecting = False
        for handler in self.handlers:
            handler.disconnect()
        self.handlers = []

    def clearHandlers(self):
        '''
        Delete all recorded records in all handlers.
        :return:
        '''
        for handler in self.handlers:
            handler.clear()

    def getAllRecords(self):
        '''
        :return: a list of all records in all handlers
        '''
        all_records = []
        for handler in self.handlers:
            handler.acquire()
            try:
                all_records.extend(handler.records)
            finally:
                handler.release()
        return all_records

    def check(self, stop=True):
        '''
        Check for Records. Raises a LogWatchException if any record was found
        :param stop: if True, stop is called internally
        '''
        all_records = self.getAllRecords()
        if stop:
            self.stop()
        if all_records:
            raise LogWatchException(all_records)

    def filter(self, record): #pylint: disable=unused-argument
        return self._collecting == True #pylint: disable=singleton-comparision

    def __enter__(self):
        return self

    def __exit__(self, et, ev, tb):
        self.check()
