"""Tests for consumer mdib handling."""

import unittest
import unittest.mock

from sdc11073 import definitions_sdc
from sdc11073.mdib import ConsumerMdib, consumermdib


class TestHasNewStateUsableStateVersion(unittest.TestCase):
    REPORT_NAME = 'SomeReport'

    def setUp(self) -> None:
        sdc_client = unittest.mock.MagicMock()
        sdc_client.sdc_definitions = definitions_sdc.SdcV1Definitions
        sdc_client.log_prefix = ''
        self.mdib = ConsumerMdib(sdc_client)
        self.mdib._logger = unittest.mock.MagicMock()

    @staticmethod
    def _mk_state(state_version: int) -> unittest.mock.MagicMock:
        state = unittest.mock.MagicMock()
        state.StateVersion = state_version
        state.DescriptorHandle = 'my_handle'
        return state

    def _raise_on_invalid_state_version(self, old_version: int, new_version: int) -> None:
        self.mdib._raise_on_invalid_state_version(
            self._mk_state(old_version),
            self._mk_state(new_version),
            self.REPORT_NAME,
        )

    def test_incremented_state_version_returns_true(self):
        self._raise_on_invalid_state_version(41, 42)

    def test_missed_state_versions_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._raise_on_invalid_state_version(41, 44)
        msg = str(ctx.exception)
        self.assertIn(self.REPORT_NAME, msg)
        self.assertIn('missed 2 states', msg)
        self.assertIn('my_handle', msg)
        self.assertIn('(41->44)', msg)

    def test_decremented_state_version_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._raise_on_invalid_state_version(42, 41)
        msg = str(ctx.exception)
        self.assertIn(self.REPORT_NAME, msg)
        self.assertIn('received older state', msg)
        self.assertIn('my_handle', msg)
        self.assertIn('(42->41)', msg)

    def test_repeated_state_version_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._raise_on_invalid_state_version(42, 42)
        msg = str(ctx.exception)
        self.assertIn(self.REPORT_NAME, msg)
        self.assertIn('received older state', msg)
        self.assertIn('my_handle', msg)
        self.assertIn('(42->42)', msg)


class TestCanAcceptMdibVersion(unittest.TestCase):
    LOG_PREFIX = 'SomePrefix'
    CURRENT_VERSION = 10

    def setUp(self) -> None:
        sdc_client = unittest.mock.MagicMock()
        sdc_client.sdc_definitions = definitions_sdc.SdcV1Definitions
        sdc_client.log_prefix = ''
        self.mdib = ConsumerMdib(sdc_client)
        self.mdib._logger = unittest.mock.MagicMock()
        self.mdib.mdib_version = self.CURRENT_VERSION

    def _can_accept(self, new_version: int) -> bool:
        return self.mdib._can_accept_mdib_version(new_version, self.LOG_PREFIX)

    def test_check_disabled_always_returns_true(self):
        self.mdib.MDIB_VERSION_CHECK_DISABLED = True
        self.assertTrue(self._can_accept(0))
        self.assertTrue(self._can_accept(self.CURRENT_VERSION))
        self.assertTrue(self._can_accept(self.CURRENT_VERSION + 5))

    def test_zero_or_negative_version_raises(self):
        for version in (0, -1, -42):
            with self.subTest(version=version):
                with self.assertRaises(ValueError) as ctx:
                    self._can_accept(version)
                exp_msg = consumermdib._MDIB_VERSION_UNEXPECTED.format(
                    self.LOG_PREFIX, self.CURRENT_VERSION + 1, version
                )
                self.assertEqual(exp_msg, str(ctx.exception))
                self.assertFalse(self.mdib._synchronized_reports.is_set())

    def test_older_version_not_synchronized_returns_false(self):
        result = self._can_accept(self.CURRENT_VERSION - 1)
        self.assertFalse(self.mdib._synchronized_reports.is_set())
        self.assertFalse(result)
        self.assertFalse(self.mdib._synchronized_reports.is_set())

    def test_older_version_synchronized_raises(self):
        self.mdib._synchronized_reports.set()
        with self.assertRaises(ValueError) as ctx:
            self._can_accept(self.CURRENT_VERSION - 1)
        exp_msg = consumermdib._MDIB_VERSION_UNEXPECTED.format(
            self.LOG_PREFIX, self.CURRENT_VERSION + 1, self.CURRENT_VERSION - 1
        )
        self.assertEqual(exp_msg, str(ctx.exception))

    def test_same_version_not_synchronized_sets_synchronized_and_returns_false(self):
        self.assertFalse(self.mdib._synchronized_reports.is_set())
        result = self._can_accept(self.CURRENT_VERSION)
        self.assertFalse(result)
        self.assertTrue(self.mdib._synchronized_reports.is_set())

    def test_same_version_already_synchronized_raises(self):
        self.mdib._synchronized_reports.set()
        with self.assertRaises(ValueError) as ctx:
            self._can_accept(self.CURRENT_VERSION)
        exp_msg = consumermdib._MDIB_VERSION_UNEXPECTED.format(
            self.LOG_PREFIX, self.CURRENT_VERSION + 1, self.CURRENT_VERSION
        )
        self.assertEqual(exp_msg, str(ctx.exception))

    def test_next_version_returns_true_and_sets_synchronized(self):
        self.assertFalse(self.mdib._synchronized_reports.is_set())
        result = self._can_accept(self.CURRENT_VERSION + 1)
        self.assertTrue(result)
        self.assertTrue(self.mdib._synchronized_reports.is_set())

    def test_next_version_already_synchronized_returns_true(self):
        self.mdib._synchronized_reports.set()
        result = self._can_accept(self.CURRENT_VERSION + 1)
        self.assertTrue(result)
        self.assertTrue(self.mdib._synchronized_reports.is_set())

    def test_version_gap_all_subscribed_raises(self):
        self.mdib._sdc_client.all_subscribed = True
        with self.assertRaises(ValueError) as ctx:
            self._can_accept(self.CURRENT_VERSION + 5)
        exp_msg = consumermdib._MDIB_VERSION_UNEXPECTED.format(
            self.LOG_PREFIX, self.CURRENT_VERSION + 1, self.CURRENT_VERSION + 5
        )
        self.assertEqual(exp_msg, str(ctx.exception))

    def test_version_gap_not_all_subscribed_returns_true_and_sets_synchronized(self):
        self.mdib._sdc_client.all_subscribed = False
        self.assertFalse(self.mdib._synchronized_reports.is_set())
        result = self._can_accept(self.CURRENT_VERSION + 5)
        self.assertTrue(result)
        self.assertTrue(self.mdib._synchronized_reports.is_set())


class TestRetrieveContextStates(unittest.TestCase):
    def setUp(self) -> None:
        sdc_client = unittest.mock.MagicMock()
        sdc_client.sdc_definitions = definitions_sdc.SdcV1Definitions
        sdc_client.log_prefix = ''
        self.mdib = ConsumerMdib(sdc_client)
        self.mdib._logger = unittest.mock.MagicMock()

    @staticmethod
    def _mk_context_state(handle: str, descriptor_handle: str) -> unittest.mock.MagicMock:
        state = unittest.mock.MagicMock()
        state.Handle = handle
        state.DescriptorHandle = descriptor_handle
        state.NODETYPE = None
        return state

    def test_raises_runtime_error_if_context_state_already_in_mdib(self):
        handle = 'ctx_handle_1'
        descriptor_handle = 'ctx_desc_1'

        existing_state = self._mk_context_state(handle, descriptor_handle)
        self.mdib.context_states.add_object(existing_state)

        incoming_state = self._mk_context_state(handle, descriptor_handle)
        response = unittest.mock.MagicMock()
        response.result.ContextState = [incoming_state]
        self.mdib._sdc_client.client.return_value.get_context_states.return_value = response

        with self.assertRaises(RuntimeError) as ctx:
            self.mdib._retrieve_context_states()

        msg = str(ctx.exception)
        self.assertIn('Unexpected behavior', msg)
        self.assertIn('ContextState(s) are already included in the MDIB', msg)
        self.assertIn('Found 1 objects', msg)
