"""Helper functions and classes for logging in sdc11073."""

import logging
import traceback
import typing
from collections.abc import Sequence
from logging import handlers as logging_handlers



def reset_log_levels(root_logger_name: str = 'sdc') -> None:
    """Reset the log level of the given logger and all its sub-loggers to NOTSET.

    :param root_logger_name: name of the root logger whose levels shall be reset
    """
    sub_logger_name = root_logger_name + '.'
    for name in logging.Logger.manager.loggerDict:
        if name.startswith(sub_logger_name) or name == root_logger_name:
            logging.getLogger(name).setLevel(logging.NOTSET)


def reset_handlers(root_logger_name: str = 'sdc') -> None:
    """Remove all handlers from the given logger and all its sub-loggers.

    :param root_logger_name: name of the root logger whose handlers shall be removed
    """
    sub_logger_name = root_logger_name + '.'
    for name in logging.Logger.manager.loggerDict:
        if name.startswith(sub_logger_name) or name == root_logger_name:
            logger = logging.getLogger(name)
            for handler in logger.handlers:
                logger.removeHandler(handler)


def basic_logging_setup(
    root_logger_name: str = 'sdc',
    level: int = logging.INFO,
    log_file_name: str | None = None,
) -> None:
    """Set up basic logging with a stream handler and an optional rotating file handler.

    :param root_logger_name: name of the logger to configure
    :param level: the log level to set on the logger
    :param log_file_name: if provided, a rotating file handler for this file is created
    """
    reset_log_levels(root_logger_name)
    reset_handlers(root_logger_name)
    logger = logging.getLogger(root_logger_name)
    logger.setLevel(level)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    if log_file_name:
        file_handler = logging_handlers.RotatingFileHandler(log_file_name, maxBytes=5000000, backupCount=2)
        file_handler.setFormatter(formatter)


class LoggerAdapter:
    """Wrap a standard logger and change the interface in two ways.

    - It uses the .format() method of strings for formatting (in contrast to logging.Logger, which uses %).
    - If any argument in *args or **kwargs is callable, it is replaced with the return value of the call.
      This reduces processing time if the call is expensive and the logger is not enabled for the log level.
    """

    def __init__(self, logger: logging.Logger, prefix: str | None = None) -> None:
        self.logger = logger
        self.log_prefix = prefix or ''

    def _process(self, msg: str, args: tuple, kwargs: dict) -> str:
        try:
            _msg = f'{self.log_prefix}{msg}'
        except TypeError:
            _msg = msg

        if len(args) == len(kwargs) == 0:
            return _msg

        if '%' in msg and '{' not in msg:
            # traditional log formatting
            return _msg % args

        resolved_args = [arg() if callable(arg) else arg for arg in args]
        resolved_kwargs = {}
        for key, arg in kwargs.items():
            resolved_kwargs[key] = arg() if callable(arg) else arg

        try:
            return _msg.format(*resolved_args, **resolved_kwargs)
        except:
            print(traceback.format_exc())
            raise

    def debug(self, msg: str, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Log a message with level DEBUG."""
        self.log(logging.DEBUG, msg, *args, **kwargs)

    def info(self, msg: str, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Log a message with level INFO."""
        self.log(logging.INFO, msg, *args, **kwargs)

    def warning(self, msg: str, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Log a message with level WARNING."""
        self.log(logging.WARNING, msg, *args, **kwargs)

    warn = warning

    def error(self, msg: str, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Log a message with level ERROR."""
        self.log(logging.ERROR, msg, *args, **kwargs)

    def exception(self, msg: str, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Log a message with level ERROR, including exception information."""
        # exc_info is inherent to this method's design; it mirrors logging.Logger.exception.
        self.logger.error(self._process(msg, args, kwargs), exc_info=True)  # noqa: LOG014

    def critical(self, msg: str, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Log a message with level CRITICAL."""
        self.log(logging.CRITICAL, msg, *args, **kwargs)

    def log(self, level: int, msg: str, *args: typing.Any, **kwargs: typing.Any) -> None:
        """Delegate a log call to the underlying logger after processing msg, args and kwargs."""
        if self.logger.isEnabledFor(level):
            self.logger.log(level, self._process(msg, args, kwargs))


def get_logger_adapter(name: str, prefix: str | None = None) -> LoggerAdapter:
    """Create a LoggerAdapter instead of using logging.getLogger.

    :param name: the name of the logger to wrap
    :param prefix: an optional prefix that is prepended to every log message
    :return: a LoggerAdapter instance
    """
    return LoggerAdapter(logging.getLogger(name), prefix)


class _LogIssue:
    def __init__(self, record: logging.LogRecord) -> None:
        self.record = record
        self.call_stack = traceback.format_stack(limit=15)
        # remove last lines from call stack that are inside logging and loghelper.
        # By doing this the call stack shows the call to the logger as last entry.
        while __file__ in self.call_stack[-1] or logging.__file__ in self.call_stack[-1]:
            del self.call_stack[-1]

    def __repr__(self) -> str:
        call_stack = ''.join(self.call_stack)
        return (
            f'log msg="{self.record.msg}" level={self.record.levelname} '
            f'thread="{self.record.threadName or self.record.thread}"; call-stack:\n{call_stack}'
        )


class LogWatchError(Exception):
    """Exception raised by LogWatcher when unexpected log records were recorded."""

    def __init__(self, issues: Sequence[_LogIssue]) -> None:
        super().__init__()
        self.issues = issues

    def __repr__(self) -> str:
        return f'LogWatchException: {self.issues}'


class LogWatcherHandler(logging.Handler):
    """Logging handler that stores all records in a list."""

    def __init__(self, logger: logging.Logger, level: int) -> None:
        """Store all records of the given logger with log level >= level.

        :param logger: the logger that shall be handled
        :param level: all records with log level >= level will be recorded
        """
        super().__init__(level=level)
        self._logger = logger
        self.records = []
        self._logger.addHandler(self)

    def emit(self, record: logging.LogRecord) -> None:
        """Store the record; called by the logger if the record log level >= own level."""
        self.acquire()
        try:
            self.records.append(_LogIssue(record))
        finally:
            self.release()

    def disconnect(self) -> None:
        """Remove self from logger."""
        self._logger.removeHandler(self)

    def clear(self) -> None:
        """Delete all records."""
        self.acquire()
        try:
            del self.records[:]
        finally:
            self.release()


class LogWatcher:
    """Manage one or more LogWatcherHandlers.

    Can be used as a context manager.
    """

    def __init__(
        self,
        logger: logging.Logger,
        level: int = logging.ERROR,
        startPaused: bool = False,  # noqa: N803
    ) -> None:
        """Record log messages of the given logger.

        :param logger: the initial logger that shall be recorded
        :param level: the log level for the initial handler
        :param startPaused: if True, recording is not started immediately
        """
        self._logger = logger
        self._level = level
        self.handlers = []
        self._collecting = False
        self.addHandler(logger, level)
        self._collecting = not startPaused

    def addHandler(self, logger: logging.Logger, level: int) -> LogWatcherHandler:  # noqa: N802
        """Add another LogWatcherHandler.

        :param logger: the logger that shall be recorded
        :param level: the log level for the handler
        :return: a LogWatcherHandler instance
        """
        coll = LogWatcherHandler(logger, level)
        coll.addFilter(self)
        self.handlers.append(coll)
        return coll

    def setPaused(self, isPaused: bool) -> None:  # noqa: N802, N803
        """Enable or disable recording.

        :param isPaused: if True, no records will be saved
        """
        self._collecting = not isPaused

    def stop(self) -> None:
        """Disconnect and delete all handlers."""
        self._collecting = False
        for handler in self.handlers:
            handler.disconnect()
        self.handlers = []

    def clearHandlers(self) -> None:  # noqa: N802
        """Delete all recorded records in all handlers."""
        for handler in self.handlers:
            handler.clear()

    # getAllRecords is public API used by callers/tests; keep the name.
    def getAllRecords(self) -> Sequence[_LogIssue]:  # noqa: N802
        """Return a list of all records in all handlers."""
        all_records = []
        for handler in self.handlers:
            handler.acquire()
            try:
                all_records.extend(handler.records)
            finally:
                handler.release()
        return all_records

    def check(self, stop: bool = True) -> None:
        """Check for records and raise a LogWatchError if any record was found.

        :param stop: if True, stop is called internally
        """
        all_records = self.getAllRecords()
        if stop:
            self.stop()
        if all_records:
            raise LogWatchError(all_records)

    def filter(self, _: logging.LogRecord) -> bool:
        """Return whether records are currently being collected."""
        return self._collecting

    def __enter__(self) -> 'LogWatcher':  # noqa: PYI034
        return self

    def __exit__(self, et: object, ev: object, tb: object) -> None:
        self.check()
