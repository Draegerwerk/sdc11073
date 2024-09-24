import logging
import unittest
import uuid
from unittest import mock

from sdc11073 import loghelper


def _run_test_cm(logger, level):
    with loghelper.LogWatcher(logger, level):
        logger.debug('123')
        logger.info('234')
        logger.warning('345')


class TestLogWatcher(unittest.TestCase):

    def test_logwatcher_contextmanager(self):
        logger = logging.getLogger('TestLogWatcher_CM')
        logger.setLevel(logging.DEBUG)
        _run_test_cm(logger, logging.ERROR)  # no exception
        self.assertRaises(loghelper.LogWatchError, _run_test_cm, logger, logging.WARN)
        try:
            _run_test_cm(logger, logging.INFO)
        except loghelper.LogWatchError as ex:
            self.assertEqual(len(ex.issues), 2)
            for e in ex.issues:
                print(e)
        else:
            self.fail('LogWatchException not raised')

    def test_logwatcher(self):
        logger = logging.getLogger('TestLogWatcher')
        logger.setLevel(logging.DEBUG)
        lw = loghelper.LogWatcher(logger, level=logging.ERROR)
        logger.debug('123')
        logger.info('234')
        logger.warning('345')
        lw.check()

        lw2 = loghelper.LogWatcher(logger, level=logging.WARNING)
        logger.debug('123')
        logger.info('234')
        lw2.setPaused(True)
        logger.warning('345')  # shall not be handled, because paused
        lw2.check(stop=False)
        lw2.setPaused(False)
        logger.warning('456')
        records = lw2.getAllRecords()
        self.assertEqual(len(records), 1)
        self.assertRaises(loghelper.LogWatchError, lw2.check)

    def test_ident_parameter(self):
        def _test_prefix(prefix):
            adapter = loghelper.LoggerAdapter(logger=mock.MagicMock(), prefix=prefix)
            msg = uuid.uuid4()
            processed_msg = adapter._process(msg, (), ())
            self.assertEqual(f'{prefix or ""}{msg}', processed_msg)

        _test_prefix(1)
        _test_prefix('1')
        _test_prefix(mock.MagicMock())
        _test_prefix(None)
