"""Tests for transactions of EntityProviderMdib."""
import pathlib
import unittest

from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.entity_mdib.entity_providermdib import EntityProviderMdib
from sdc11073.exceptions import ApiUsageError
from sdc11073.xml_types import pm_qnames, pm_types

mdib_file = str(pathlib.Path(__file__).parent.joinpath('mdib_tns.xml'))


class TestEntityTransactions(unittest.TestCase):
    """Test all kinds of transactions for entity interface of EntityProviderMdib."""

    def setUp(self):   # noqa: D102
        self._mdib = EntityProviderMdib.from_mdib_file(mdib_file,
                                                 protocol_definition=SdcV1Definitions)

    def test_alert_state_update(self):
        """Verify that alert_state_transaction works as expected.

        - mdib_version is incremented
        - StateVersion is incremented in mdib state
        - updated state is referenced in transaction_result
        - observable alert_by_handle is updated
        - ApiUsageError is thrown if state of wrong kind is added,
        """
        mdib_version = self._mdib.mdib_version
        old_ac_entity = self._mdib.entities.node_type(pm_qnames.AlertConditionDescriptor)[0]
        old_ac_entity.state.Presence = True
        with self._mdib.alert_state_transaction() as mgr:
            mgr.write_entity(old_ac_entity)
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)
        self.assertEqual(len(self._mdib.transaction.alert_updates), 1)  # this causes an EpisodicAlertReport

        new_ac_entity = self._mdib.entities.handle(old_ac_entity.handle)
        self.assertEqual(new_ac_entity.state.StateVersion, old_ac_entity.state.StateVersion + 1)

        metric_entities = self._mdib.entities.node_type(pm_qnames.NumericMetricDescriptor)
        with self._mdib.alert_state_transaction() as mgr:
            self.assertRaises(ApiUsageError, mgr.write_entity, metric_entities[0])
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)

    def test_metric_state_update(self):
        """Verify that metric_state_transaction works as expected.

        - mdib_version is incremented
        - StateVersion is incremented in mdib state
        - updated state is referenced in transaction_result
        - observable metric_by_handle is updated
        - ApiUsageError is thrown if state of wrong kind is added,
        """
        mdib_version = self._mdib.mdib_version
        old_metric_entity = self._mdib.entities.node_type(pm_qnames.NumericMetricDescriptor)[0]
        old_metric_entity.state.LifeTimePeriod = 2
        with self._mdib.metric_state_transaction() as mgr:
            mgr.write_entity(old_metric_entity)
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)
        self.assertEqual(len(self._mdib.transaction.metric_updates), 1)

        new_metric_entity = self._mdib.entities.handle(old_metric_entity.handle)
        self.assertEqual(new_metric_entity.state.StateVersion, old_metric_entity.state.StateVersion + 1)

        ac_entities = self._mdib.entities.node_type(pm_qnames.AlertConditionDescriptor)
        with self._mdib.metric_state_transaction() as mgr:
            self.assertRaises(ApiUsageError, mgr.write_entity, ac_entities[0])
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)

    def test_operational_state_update(self):
        """Verify that operational_state_transaction works as expected.

        - mdib_version is incremented
        - StateVersion is incremented in mdib state
        - updated state is referenced in transaction_result
        - observable operation_by_handle is updated
        - ApiUsageError is thrown if state of wrong kind is added,
        """
        mdib_version = self._mdib.mdib_version
        old_op_entity = self._mdib.entities.node_type(pm_qnames.SetAlertStateOperationDescriptor)[0]
        old_op_entity.state.OperationMode = pm_types.OperatingMode.DISABLED
        with self._mdib.operational_state_transaction() as mgr:
            mgr.write_entity(old_op_entity)
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)
        self.assertEqual(len(self._mdib.transaction.op_updates), 1)
        self.assertTrue(old_op_entity.handle in self._mdib.operation_by_handle)

        new_op_entity = self._mdib.entities.handle(old_op_entity.handle)
        self.assertEqual(new_op_entity.state.StateVersion, old_op_entity.state.StateVersion + 1)

        metric_entities = self._mdib.entities.node_type(pm_qnames.NumericMetricDescriptor)
        with self._mdib.alert_state_transaction() as mgr:
            self.assertRaises(ApiUsageError, mgr.write_entity, metric_entities[0])
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)

    def test_context_state_transaction(self):
        """Verify that context_state_transaction works as expected.

        - mk_context_state method works as expected
        - mdib_version is incremented
        - StateVersion is incremented in mdib state
        - updated state is referenced in transaction_result
        - observable context_by_handle is updated
        - ApiUsageError is thrown if state of wrong kind is added
        """
        mdib_version = self._mdib.mdib_version
        old_pat_entity = self._mdib.entities.node_type(pm_qnames.PatientContextDescriptor)[0]
        new_state = old_pat_entity.new_state()
        self.assertIsNotNone(new_state.Handle)
        new_state.CoreData.Givenname = 'foo'
        new_state.CoreData.Familyname = 'bar'
        context_handle = new_state.Handle

        with self._mdib.context_state_transaction() as mgr:
            mgr.write_entity(old_pat_entity, [context_handle])
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)
        self.assertEqual(len(self._mdib.transaction.ctxt_updates), 1)

        new_pat_entity = self._mdib.entities.handle(old_pat_entity.handle)
        self.assertEqual(new_pat_entity.states[context_handle].StateVersion,0)
        self.assertEqual(new_pat_entity.states[context_handle].CoreData.Givenname,'foo')
        self.assertEqual(new_pat_entity.states[context_handle].CoreData.Familyname,'bar')

        new_pat_entity.states[context_handle].CoreData.Familyname = 'foobar'

        with self._mdib.context_state_transaction() as mgr:
            mgr.write_entity(new_pat_entity, [context_handle])

        newest_pat_entity =  self._mdib.entities.handle(old_pat_entity.handle)
        self.assertEqual(newest_pat_entity.states[context_handle].StateVersion,1)
        self.assertEqual(newest_pat_entity.states[context_handle].CoreData.Familyname,'foobar')

        metric_entities = self._mdib.entities.node_type(pm_qnames.NumericMetricDescriptor)
        with self._mdib.alert_state_transaction() as mgr:
            self.assertRaises(ApiUsageError, mgr.write_entity, metric_entities[0])
        self.assertEqual(mdib_version + 2, self._mdib.mdib_version)

    def test_description_modification(self):
        """Verify that descriptor_transaction works as expected.

        - mdib_version is incremented
        - observable updated_descriptors_by_handle is updated
        - corresponding states for descriptor modifications are also updated
        - ApiUsageError is thrown if data of wrong kind is requested
        """
        mdib_version = self._mdib.mdib_version
        old_ac_entity = self._mdib.entities.node_type(pm_qnames.AlertConditionDescriptor)[0]
        old_metric_entity = self._mdib.entities.node_type(pm_qnames.NumericMetricDescriptor)[0]
        old_op_entity = self._mdib.entities.node_type(pm_qnames.SetAlertStateOperationDescriptor)[0]
        old_comp_entity = self._mdib.entities.node_type(pm_qnames.ChannelDescriptor)[0]
        old_rt_entity = self._mdib.entities.node_type(pm_qnames.RealTimeSampleArrayMetricDescriptor)[0]
        old_ctx_entity = self._mdib.entities.node_type(pm_qnames.PatientContextDescriptor)[0]

        with self._mdib.descriptor_transaction() as mgr:
            # verify that updating descriptors of different kinds and accessing corresponding states works
            mgr.write_entity(old_ac_entity)
            mgr.write_entity(old_metric_entity)
            mgr.write_entity(old_op_entity)
            mgr.write_entity(old_comp_entity)
            mgr.write_entity(old_rt_entity)
            mgr.write_entity(old_ctx_entity)
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)
        transaction_result = self._mdib.transaction
        self.assertEqual(len(transaction_result.metric_updates), 1)
        self.assertEqual(len(transaction_result.alert_updates), 1)
        self.assertEqual(len(transaction_result.op_updates), 1)
        self.assertEqual(len(transaction_result.comp_updates), 1)
        self.assertEqual(len(transaction_result.rt_updates), 1)
        self.assertEqual(len(transaction_result.ctxt_updates), 1)
        self.assertEqual(len(transaction_result.descr_updated), 6)

        self.assertTrue(old_ac_entity.handle in self._mdib.updated_descriptors_by_handle)
        self.assertTrue(old_ac_entity.handle in self._mdib.alert_by_handle)
        self.assertTrue(old_metric_entity.handle in self._mdib.updated_descriptors_by_handle)
        self.assertTrue(old_metric_entity.handle in self._mdib.metrics_by_handle)
        self.assertTrue(old_op_entity.handle in self._mdib.updated_descriptors_by_handle)
        self.assertTrue(old_op_entity.handle in self._mdib.operation_by_handle)
        self.assertTrue(old_comp_entity.handle in self._mdib.updated_descriptors_by_handle)
        self.assertTrue(old_comp_entity.handle in self._mdib.component_by_handle)
        self.assertTrue(old_rt_entity.handle in self._mdib.updated_descriptors_by_handle)

    def test_remove_add(self):
        """Verify that removing descriptors / states and adding them later again results in correct versions."""
        # remove all root descriptors
        all_entities = {}
        for handle in self._mdib._entities: # noqa: SLF001
            all_entities[handle] = self._mdib.entities.handle(handle) # get external representation

        root_entities = self._mdib.entities.parent_handle(None)
        with self._mdib.descriptor_transaction() as mgr:
            for ent in root_entities:
                mgr.remove_entity(ent)

        self.assertEqual(0, len(self._mdib._entities)) # noqa: SLF001

        # add all entities again
        with self._mdib.descriptor_transaction() as mgr:
            mgr.write_entities(all_entities.values())

        # verify that the number of entities is the same as before
        self.assertEqual(len(all_entities), len(self._mdib.internal_entities))

        # verify that all descriptors and states have incremented version counters
        for current_ent in self._mdib.internal_entities.values():
            old_ent = all_entities[current_ent.handle]
            self.assertEqual(current_ent.descriptor.DescriptorVersion, old_ent.descriptor.DescriptorVersion + 1)
            if current_ent.is_multi_state:
                for handle, current_state in current_ent.states.items():
                    old_state = old_ent.states[handle]
                    self.assertEqual(current_state.StateVersion, old_state.StateVersion + 1)
                    self.assertEqual(current_state.DescriptorVersion, current_ent.descriptor.DescriptorVersion)
            else:
                self.assertEqual(current_ent.state.StateVersion, old_ent.state.StateVersion + 1)
                self.assertEqual(current_ent.state.DescriptorVersion, current_ent.descriptor.DescriptorVersion)
