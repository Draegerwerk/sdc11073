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

    def _clear_logs(self):
        self.mdib._logger.log.reset_mock()
        self.mdib._logger.error.reset_mock()
        self.mdib._logger.warning.reset_mock()

    def test_can_accept_mdib_version_description_modification(self):
        # initial state check
        self.assertEqual(None, self.mdib._last_descr_modification_mdib_version)
        self.assertFalse(self.mdib._synchronizedReports.is_set())

        # report with valid mdib version -> synchronized reports are activated
        self.assertTrue(self.mdib._canAcceptMdibVersion(log_prefix=self.logger_prefix, newMdibVersion=11))
        self.mdib.mdibVersion = 11
        self.assertIsNone(self.mdib._last_descr_modification_mdib_version)
        self.assertTrue(self.mdib._synchronizedReports.is_set())

        # valid DescriptionModifiaction
        self.assertTrue(self.mdib._canAcceptMdibVersion(
                log_prefix=self.logger_prefix, newMdibVersion=12, is_description_modification=True))
        self.mdib.mdibVersion = 12
        self.assertEqual(12, self.mdib._last_descr_modification_mdib_version)
        self._assert_logs_not_called()

        # valid report (state modification) with same MdibVersion as previous DescriptionModification
        self.assertTrue(self.mdib._canAcceptMdibVersion(log_prefix=self.logger_prefix, newMdibVersion=12))
        self.assertEqual(12, self.mdib._last_descr_modification_mdib_version)
        self.assertEqual(12, self.mdib.mdibVersion)
        self._assert_logs_not_called()

        # again a valid report (state modification) with same MdibVersion as previous DescriptionModification
        self.assertTrue(self.mdib._canAcceptMdibVersion(log_prefix=self.logger_prefix, newMdibVersion=12))
        self.assertEqual(12, self.mdib._last_descr_modification_mdib_version)
        self.assertEqual(12, self.mdib.mdibVersion)
        self._assert_logs_not_called()

        # valid report (state modification)
        self.assertTrue(self.mdib._canAcceptMdibVersion(log_prefix=self.logger_prefix, newMdibVersion=13))
        self.mdib.mdibVersion = 13
        self.assertIsNone(self.mdib._last_descr_modification_mdib_version)
        self._assert_logs_not_called()

        # invalid report (state modification) with same MdibVersion as previous report (state modification)
        self.assertTrue(self.mdib._canAcceptMdibVersion(log_prefix=self.logger_prefix, newMdibVersion=13))
        self.assertIsNone(self.mdib._last_descr_modification_mdib_version)
        self.assertEqual(13, self.mdib.mdibVersion)
        self.mdib._logger.error.assert_called_with(clientmdib.MDIB_VERSION_NOT_ALLOWED, mock.ANY, mock.ANY, mock.ANY)
        self._clear_logs()
        self._assert_logs_not_called()

        # a valid report (state modification)
        self.assertTrue(self.mdib._canAcceptMdibVersion(log_prefix=self.logger_prefix, newMdibVersion=14))
        self.mdib.mdibVersion = 14
        self.assertIsNone(self.mdib._last_descr_modification_mdib_version)
        self._assert_logs_not_called()

        # invalid DescriptionModifiaction with same MdibVersion as previous motofication
        self.assertTrue(self.mdib._canAcceptMdibVersion(
            log_prefix=self.logger_prefix, newMdibVersion=14, is_description_modification=True))
        self.mdib._logger.error.assert_called_with(clientmdib.MDIB_VERSION_NOT_ALLOWED, mock.ANY, mock.ANY, mock.ANY)
        self.assertIsNone(self.mdib._last_descr_modification_mdib_version)

    def test_can_accept_mdib_version_invalid_version(self):
        # initial state check
        self.assertIsNone(self.mdib._last_descr_modification_mdib_version)
        self.assertFalse(self.mdib._synchronizedReports.is_set())

        # report with old mdib version before synchronized reports are activated -> logger warning
        self.assertFalse(self.mdib._canAcceptMdibVersion(log_prefix=self.logger_prefix, newMdibVersion=9))
        self.assertIsNone(self.mdib._last_descr_modification_mdib_version)
        self.assertFalse(self.mdib._synchronizedReports.is_set())
        self.mdib._logger.log.assert_called_with(logging.WARNING, clientmdib.MDIB_VERSION_TOO_OLD,
                                                 mock.ANY, mock.ANY, mock.ANY)
        self._clear_logs()
        self._assert_logs_not_called()

        # report with valid mdib version -> synchronized reports are activated
        self.assertTrue(self.mdib._canAcceptMdibVersion(log_prefix=self.logger_prefix, newMdibVersion=11))
        self.mdib.mdibVersion = 11
        self.assertIsNone(self.mdib._last_descr_modification_mdib_version)
        self.assertTrue(self.mdib._synchronizedReports.is_set())
        self._assert_logs_not_called()

        # report with old mdib version with synchronized reports are activated -> logger error
        self.assertFalse(self.mdib._canAcceptMdibVersion(log_prefix=self.logger_prefix, newMdibVersion=0))
        self.assertIsNone(self.mdib._last_descr_modification_mdib_version)
        self.assertTrue(self.mdib._synchronizedReports.is_set())
        self.mdib._logger.log.assert_called_with(logging.ERROR, clientmdib.MDIB_VERSION_TOO_OLD,
                                                 mock.ANY, mock.ANY, mock.ANY)
        self._clear_logs()
        self._assert_logs_not_called()

        # report with valid mdib version -> synchronized reports are activated
        self.assertTrue(self.mdib._canAcceptMdibVersion(log_prefix=self.logger_prefix, newMdibVersion=12))
        self.mdib.mdibVersion = 12
        self.assertIsNone(self.mdib._last_descr_modification_mdib_version)
        self.assertTrue(self.mdib._synchronizedReports.is_set())
        self._assert_logs_not_called()

        # invalid report (state modification) with new MdibVersion incremented by more than 1
        self.assertTrue(self.mdib._canAcceptMdibVersion(log_prefix=self.logger_prefix, newMdibVersion=30))
        self.mdib._logger.error.assert_called_with(clientmdib.MDIB_VERSION_UNEXPECTED, mock.ANY, mock.ANY, mock.ANY)
        self._clear_logs()
        self._assert_logs_not_called()

    def test_can_accept_mdib_version_initialization(self):
        """same MdibVersion is allowed during initialization process -> no error/warnings shall be logged"""
        self.assertFalse(self.mdib._synchronizedReports.is_set())
        # report with old mdib version before synchronized reports are activated -> logger warning
        self.assertTrue(
            self.mdib._canAcceptMdibVersion(log_prefix=self.logger_prefix, newMdibVersion=self.mdib.mdibVersion))
        self.assertIsNone(self.mdib._last_descr_modification_mdib_version)
        self.assertTrue(self.mdib._synchronizedReports.is_set())
        self._assert_logs_not_called()
