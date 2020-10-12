import unittest
import logging
from sdc11073 import loghelper


class TestLogWatcher(unittest.TestCase):

    def _runtest_CM(self, logger, level):
        with loghelper.LogWatcher(logger, level) as w:
            logger.debug('123')
            logger.info('234')
            logger.warning('345')

    def test_logwatcher_contextmanager(self):
        logger = logging.getLogger('TestLogWatcher_CM')
        logger.setLevel(logging.DEBUG)
        self._runtest_CM(logger, logging.ERROR) # no exception
        self.assertRaises(loghelper.LogWatchException, self._runtest_CM, logger, logging.WARN)
        try:
            exc = self._runtest_CM(logger, logging.INFO)
        except loghelper.LogWatchException as ex:
            self.assertEqual(len(ex.issues), 2)
            for e in ex.issues:
                print (e)
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
        logger.warning('345') # shall not be handled, because paused
        lw2.check(stop=False)
        lw2.setPaused(False)
        logger.warning('456')# shall be handle
        records = lw2.getAllRecords()
        self.assertEqual(len(records), 1)
        self.assertRaises(loghelper.LogWatchException, lw2.check)
