import logging
from unittest import TestCase
from unittest import mock

from sdc11073.mdib import clientmdib


class TestClientMidb(TestCase):
    def setUp(self) -> None:
        sdc_client = mock.MagicMock()
        sdc_client.all_subscribed = True
        self.mdib = clientmdib.ClientMdibContainer(sdcClient=sdc_client)
        self.mdib.mdibVersion = 10

        self.logger_prefix = "mock_logger"
        self.mdib._logger = mock.MagicMock()

    def _assert_logs_not_called(self):
        self.mdib._logger.log.assert_not_called()
        self.mdib._logger.error.assert_not_called()
        self.mdib._logger.warning.assert_not_called()

    def test_negative_mdib_version_raises(self):
        with self.assertRaises(ValueError):
            self.mdib._canAcceptMdibVersion(self.logger_prefix, -1)

    def test_zero_mdib_version_raises(self):
        with self.assertRaises(ValueError):
            self.mdib._canAcceptMdibVersion(self.logger_prefix, 0)

    def test_older_mdib_version_synchronized_raises(self):
        self.mdib._synchronizedReports.set()
        with self.assertRaises(ValueError):
            self.mdib._canAcceptMdibVersion(self.logger_prefix, self.mdib.mdibVersion - 1)

    def test_older_mdib_version_not_synchronized_returns_false(self):
        self.assertFalse(self.mdib._synchronizedReports.is_set())

        result = self.mdib._canAcceptMdibVersion(self.logger_prefix, self.mdib.mdibVersion - 1)

        self.assertFalse(result)
        self.mdib._logger.debug.assert_called_once()
        self.assertFalse(self.mdib._synchronizedReports.is_set())

    def test_equal_mdib_version_synchronized_raises(self):
        self.mdib._synchronizedReports.set()
        with self.assertRaises(ValueError):
            self.mdib._canAcceptMdibVersion(self.logger_prefix, self.mdib.mdibVersion)

    def test_equal_mdib_version_not_synchronized_sets_synchronized(self):
        self.assertFalse(self.mdib._synchronizedReports.is_set())

        result = self.mdib._canAcceptMdibVersion(self.logger_prefix, self.mdib.mdibVersion)

        self.assertFalse(result)
        self.assertTrue(self.mdib._synchronizedReports.is_set())
        self._assert_logs_not_called()

    def test_gap_mdib_version_not_all_subscribed_returns_true(self):
        self.mdib._sdcClient.all_subscribed = False
        self.assertFalse(self.mdib._synchronizedReports.is_set())

        result = self.mdib._canAcceptMdibVersion(self.logger_prefix, self.mdib.mdibVersion + 2)

        self.assertTrue(result)
        self.assertTrue(self.mdib._synchronizedReports.is_set())
        self.mdib._logger.error.assert_called_once()

    def test_gap_mdib_version_all_subscribed_raises(self):
        self.mdib._sdcClient.all_subscribed = True
        with self.assertRaises(ValueError):
            self.mdib._canAcceptMdibVersion(self.logger_prefix, self.mdib.mdibVersion + 2)
        # error is logged before the exception is raised
        self.mdib._logger.error.assert_called_once()

    def test_next_mdib_version_returns_true(self):
        self.assertFalse(self.mdib._synchronizedReports.is_set())

        result = self.mdib._canAcceptMdibVersion(self.logger_prefix, self.mdib.mdibVersion + 1)

        self.assertTrue(result)
        self.assertTrue(self.mdib._synchronizedReports.is_set())
        self._assert_logs_not_called()


class TestHasNewStateUsableStateVersion(TestCase):
    REPORT_NAME = "SomeReport"

    def setUp(self) -> None:
        sdc_client = mock.MagicMock()
        sdc_client.all_subscribed = True
        self.mdib = clientmdib.ClientMdibContainer(sdcClient=sdc_client)
        self.mdib._logger = mock.MagicMock()

    @staticmethod
    def _mkState(state_version, diffs=None):
        state = mock.MagicMock()
        state.StateVersion = state_version
        state.descriptorHandle = "my_handle"
        return state

    def _hasNewStateUsableStateVersion(self, old_version, new_version, is_buffered_report=False):
        return self.mdib._hasNewStateUsableStateVersion(self._mkState(old_version),
                                                        self._mkState(new_version),
                                                        self.REPORT_NAME,
                                                        is_buffered_report)

    def test_incremented_state_version_returns_true(self):
        self.assertTrue(self._hasNewStateUsableStateVersion(41, 42))

    # a gap in @StateVersion means that data was missed => ValueError
    def test_missed_state_versions_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._hasNewStateUsableStateVersion(41, 44)
        msg = str(ctx.exception)
        self.assertIn(self.REPORT_NAME, msg)
        self.assertIn("missed 2 state version(s)", msg)
        self.assertIn("my_handle", msg)
        self.assertIn("expected 42", msg)

    # a decremented @StateVersion is not allowed
    def test_decremented_state_version_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._hasNewStateUsableStateVersion(42, 41)
        msg = str(ctx.exception)
        self.assertIn(self.REPORT_NAME, msg)
        self.assertIn("unexpected @StateVersion 41", msg)
        self.assertIn("current @StateVersion is 42", msg)

    def test_repeated_state_version(self):
        with self.assertRaises(ValueError) as ctx:
            self._hasNewStateUsableStateVersion(42, 42)
        msg = str(ctx.exception)
        self.assertIn("unexpected @StateVersion 42", msg)
        self.assertIn("current @StateVersion is 42", msg)

    # buffered reports may contain states that are already part of the initially received mdib
    def test_buffered_report_with_old_state_version_returns_false(self):
        self.assertFalse(self._hasNewStateUsableStateVersion(42, 41, is_buffered_report=True))

    # a gap is an error even for a buffered report
    def test_buffered_report_with_missed_state_versions_raises(self):
        with self.assertRaises(ValueError):
            self._hasNewStateUsableStateVersion(41, 44, is_buffered_report=True)
