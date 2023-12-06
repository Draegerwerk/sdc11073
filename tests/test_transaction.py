import unittest
from sdc11073.mdib.providermdib import ProviderMdib
from sdc11073.xml_types import pm_qnames, pm_types
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.exceptions import ApiUsageError
from sdc11073.mdib.modulartransactions import mk_transaction
mdib_file = 'mdib_tns.xml'


class TestTransactions(unittest.TestCase):
    """Test all kinds of transactions."""

    def setUp(self):
        self._mdib = ProviderMdib.from_mdib_file(mdib_file,
                                                 protocol_definition=SdcV1Definitions)
        self._mdib._transaction_factory = mk_transaction

    def test_alert_state_update(self):
        """Verify that alert_state_transaction works as expected.

        - mdib_version is incremented
        - StateVersion is incremented in mdib state
        - updated state is referenced in transaction_result
        - ApiUsageError is thrown if state of wrong kind is added,
        """
        self.mdib_version = self._mdib.mdib_version
        alert_conditions = self._mdib.descriptions.NODETYPE.get(pm_qnames.AlertConditionDescriptor)
        metrics = self._mdib.descriptions.NODETYPE.get(pm_qnames.NumericMetricDescriptor)
        old_state = self._mdib.states.descriptor_handle.get_one(alert_conditions[0].Handle).mk_copy()
        state_version = old_state.StateVersion
        with self._mdib.alert_state_transaction() as mgr:
            state = mgr.get_state(alert_conditions[0].Handle)
            state.Presence = True
        self.assertEqual(self.mdib_version+1, self._mdib.mdib_version)
        updated_state = self._mdib.states.descriptor_handle.get_one(alert_conditions[0].Handle)
        self.assertEqual(state_version+1, updated_state.StateVersion)
        transaction_result = self._mdib.transaction
        self.assertEqual(len(transaction_result.alert_updates), 1)  # this causes an EpisodicAlertReport
        self.assertEqual(state_version+1, transaction_result.alert_updates[0].StateVersion)

        with self._mdib.alert_state_transaction() as mgr:
            self.assertRaises(ApiUsageError, mgr.get_state, metrics[0].Handle)
        self.assertEqual(self.mdib_version+1, self._mdib.mdib_version)

    def test_metric_state_update(self):
        """Verify that metric_state_transaction works as expected.

        - mdib_version is incremented
        - StateVersion is incremented in mdib state
        - updated state is referenced in transaction_result
        - ApiUsageError is thrown if state of wrong kind is added,
        """
        self.mdib_version = self._mdib.mdib_version
        alert_conditions = self._mdib.descriptions.NODETYPE.get(pm_qnames.AlertConditionDescriptor)
        metrics = self._mdib.descriptions.NODETYPE.get(pm_qnames.NumericMetricDescriptor)
        old_state = self._mdib.states.descriptor_handle.get_one(metrics[0].Handle).mk_copy()
        state_version = old_state.StateVersion

        with self._mdib.metric_state_transaction() as mgr:
            state = mgr.get_state(metrics[0].Handle)
            state.LifeTimePeriod = 2
        self.assertEqual(self.mdib_version+1, self._mdib.mdib_version)
        updated_state = self._mdib.states.descriptor_handle.get_one(metrics[0].Handle)
        self.assertEqual(state_version+1, updated_state.StateVersion)

        transaction_result = self._mdib.transaction
        self.assertEqual(len(transaction_result.metric_updates), 1)
        self.assertEqual(state_version+1, transaction_result.metric_updates[0].StateVersion)

        with self._mdib.metric_state_transaction() as mgr:
            self.assertRaises(ApiUsageError, mgr.get_state, alert_conditions[0].Handle)
        self.assertEqual(self.mdib_version+1, self._mdib.mdib_version)

    def test_operational_state_update(self):
        """Verify that operational_state_transaction works as expected.

        - mdib_version is incremented
        - StateVersion is incremented in mdib state
        - updated state is referenced in transaction_result
        - ApiUsageError is thrown if state of wrong kind is added,
        """
        self.mdib_version = self._mdib.mdib_version
        op_descriptors = self._mdib.descriptions.NODETYPE.get(pm_qnames.SetAlertStateOperationDescriptor)
        metrics = self._mdib.descriptions.NODETYPE.get(pm_qnames.NumericMetricDescriptor)
        old_state = self._mdib.states.descriptor_handle.get_one(op_descriptors[0].Handle).mk_copy()
        state_version = old_state.StateVersion

        with self._mdib.operational_state_transaction() as mgr:
            state = mgr.get_state(op_descriptors[0].Handle)
            state.OperationMode = pm_types.OperatingMode.DISABLED
        self.assertEqual(self.mdib_version+1, self._mdib.mdib_version)
        updated_state = self._mdib.states.descriptor_handle.get_one(op_descriptors[0].Handle)
        self.assertEqual(state_version+1, updated_state.StateVersion)

        transaction_result = self._mdib.transaction
        self.assertEqual(len(transaction_result.op_updates), 1)
        self.assertEqual(state_version+1, transaction_result.op_updates[0].StateVersion)

        with self._mdib.operational_state_transaction() as mgr:
            self.assertRaises(ApiUsageError, mgr.get_state, metrics[0].Handle)
        self.assertEqual(self.mdib_version+1, self._mdib.mdib_version)

    def text_context_state_transaction(self):
        """Verify that context_state_transaction works as expected.

        - mk_context_state method works as expected
        - mdib_version is incremented
        - StateVersion is incremented in mdib state
        - updated state is referenced in transaction_result
        - ApiUsageError is thrown if state of wrong kind is added
        """
        self.mdib_version = self._mdib.mdib_version
        location_descr = self._mdib.descriptions.NODETYPE.get(pm_qnames.LocationContextDescriptor)

        with self._mdib.context_state_transaction() as mgr:
            state = mgr.mk_context_state(location_descr[0].Handle)
            state.Givenname = 'foo'
            state.Familyname = 'bar'
        self.assertIsNotNone(state.Handle)
        self.assertEqual(self.mdib_version+1, self._mdib.mdib_version)

        transaction_processor = self._mdib.transaction
        self.assertEqual(len(transaction_processor.ctxt_updates), 1)
        self.assertEqual(transaction_processor.ctxt_updates[0].StateVersion, 0)
        self.assertEqual(transaction_processor.ctxt_updates[0].Givenname, 'foo')
        self.assertEqual(transaction_processor.ctxt_updates[0].Familyname, 'bar')

        handle = transaction_processor.ctxt_updates[0].Handle
        with self._mdib.context_state_transaction() as mgr:
            state = mgr.get_context_state(handle)
        self.assertEqual(self.mdib_version+2, self._mdib.mdib_version)
        transaction_processor = self._mdib.transaction
        self.assertEqual(len(transaction_processor.ctxt_updates), 1)


    def test_description_modification(self):
        self.mdib_version = self._mdib.mdib_version
        alert_conditions = self._mdib.descriptions.NODETYPE.get(pm_qnames.AlertConditionDescriptor)
        metrics = self._mdib.descriptions.NODETYPE.get(pm_qnames.NumericMetricDescriptor)
        with self._mdib.descriptor_transaction() as mgr:
            # verify that updating descriptors of different kinds is possibler
            descr = mgr.get_descriptor(alert_conditions[0].Handle)
            state = mgr.get_state(alert_conditions[0].Handle)
            descr2 = mgr.get_descriptor(metrics[0].Handle)
            state2 = mgr.get_state(metrics[0].Handle)
        self.assertEqual(self.mdib_version+1, self._mdib.mdib_version)
        transaction_processor = self._mdib.transaction
        self.assertEqual(len(transaction_processor.metric_updates), 1)
        self.assertEqual(len(transaction_processor.alert_updates), 1)
        self.assertEqual(len(transaction_processor.descr_updated), 2)  # parent is also updated

        # verify that accessing a state for that the descriptor is not part of transaction is not allowed
        with self._mdib.descriptor_transaction() as mgr:
            descr = mgr.get_descriptor(alert_conditions[0].Handle)
            self.assertRaises(ApiUsageError, mgr.get_state, metrics[0].Handle)
        self.assertEqual(self.mdib_version+2, self._mdib.mdib_version)
