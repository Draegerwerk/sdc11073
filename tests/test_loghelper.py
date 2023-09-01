import logging
import unittest

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
