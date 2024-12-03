"""The module contains tests for transactions for ProviderMdib.

It tests classic transactions and entity transactions.
"""
import pathlib
import unittest

from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.exceptions import ApiUsageError
from sdc11073.mdib.providermdib import ProviderMdib
from sdc11073.mdib.statecontainers import NumericMetricStateContainer
from sdc11073.mdib.transactions import mk_transaction
from sdc11073.xml_types import pm_qnames, pm_types

mdib_file = str(pathlib.Path(__file__).parent.joinpath('mdib_tns.xml'))

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
        - observable alert_by_handle is updated
        - ApiUsageError is thrown if state of wrong kind is added,
        """
        mdib_version = self._mdib.mdib_version
        alert_condition_handle = self._mdib.descriptions.NODETYPE.get(pm_qnames.AlertConditionDescriptor)[0].Handle
        metrics = self._mdib.descriptions.NODETYPE.get(pm_qnames.NumericMetricDescriptor)
        old_state = self._mdib.states.descriptor_handle.get_one(alert_condition_handle).mk_copy()
        state_version = old_state.StateVersion
        with self._mdib.alert_state_transaction() as mgr:
            state = mgr.get_state(alert_condition_handle)
            state.Presence = True
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)
        updated_state = self._mdib.states.descriptor_handle.get_one(alert_condition_handle)
        self.assertEqual(state_version + 1, updated_state.StateVersion)
        transaction_result = self._mdib.transaction
        self.assertEqual(len(transaction_result.alert_updates), 1)  # this causes an EpisodicAlertReport
        self.assertEqual(state_version + 1, transaction_result.alert_updates[0].StateVersion)

        self.assertTrue(alert_condition_handle in self._mdib.alert_by_handle)

        with self._mdib.alert_state_transaction() as mgr:
            self.assertRaises(ApiUsageError, mgr.get_state, metrics[0].Handle)
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
        alert_condition_handle = self._mdib.descriptions.NODETYPE.get(pm_qnames.AlertConditionDescriptor)[0].Handle
        metric_handle = self._mdib.descriptions.NODETYPE.get(pm_qnames.NumericMetricDescriptor)[0].Handle
        old_state = self._mdib.states.descriptor_handle.get_one(metric_handle).mk_copy()
        state_version = old_state.StateVersion

        with self._mdib.metric_state_transaction() as mgr:
            state = mgr.get_state(metric_handle)
            state.LifeTimePeriod = 2
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)
        updated_state = self._mdib.states.descriptor_handle.get_one(metric_handle)
        self.assertEqual(state_version + 1, updated_state.StateVersion)

        transaction_result = self._mdib.transaction
        self.assertEqual(len(transaction_result.metric_updates), 1)
        self.assertEqual(state_version + 1, transaction_result.metric_updates[0].StateVersion)

        self.assertTrue(metric_handle in self._mdib.metrics_by_handle)

        with self._mdib.metric_state_transaction() as mgr:
            self.assertRaises(ApiUsageError, mgr.get_state, alert_condition_handle)
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
        op_descriptor_handle = self._mdib.descriptions.NODETYPE.get(
            pm_qnames.SetAlertStateOperationDescriptor)[0].Handle
        metric_handle = self._mdib.descriptions.NODETYPE.get(pm_qnames.NumericMetricDescriptor)[0].Handle
        old_state = self._mdib.states.descriptor_handle.get_one(op_descriptor_handle).mk_copy()
        state_version = old_state.StateVersion

        with self._mdib.operational_state_transaction() as mgr:
            state = mgr.get_state(op_descriptor_handle)
            state.OperationMode = pm_types.OperatingMode.DISABLED
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)
        updated_state = self._mdib.states.descriptor_handle.get_one(op_descriptor_handle)
        self.assertEqual(state_version + 1, updated_state.StateVersion)

        transaction_result = self._mdib.transaction
        self.assertEqual(len(transaction_result.op_updates), 1)
        self.assertEqual(state_version + 1, transaction_result.op_updates[0].StateVersion)

        self.assertTrue(op_descriptor_handle in self._mdib.operation_by_handle)

        with self._mdib.operational_state_transaction() as mgr:
            self.assertRaises(ApiUsageError, mgr.get_state, metric_handle)
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
        location_descr_handle = self._mdib.descriptions.NODETYPE.get(pm_qnames.LocationContextDescriptor)[0].Handle

        with self._mdib.context_state_transaction() as mgr:
            state = mgr.mk_context_state(location_descr_handle)
            state.Givenname = 'foo'
            state.Familyname = 'bar'
        self.assertIsNotNone(state.Handle)
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)

        transaction_result = self._mdib.transaction
        self.assertEqual(len(transaction_result.ctxt_updates), 1)
        self.assertEqual(transaction_result.ctxt_updates[0].StateVersion, 0)
        self.assertEqual(transaction_result.ctxt_updates[0].Givenname, 'foo')
        self.assertEqual(transaction_result.ctxt_updates[0].Familyname, 'bar')

        ctxt_handle = transaction_result.ctxt_updates[0].Handle
        with self._mdib.context_state_transaction() as mgr:
            state = mgr.get_context_state(ctxt_handle)
        self.assertEqual(mdib_version + 2, self._mdib.mdib_version)
        transaction_result = self._mdib.transaction
        self.assertEqual(len(transaction_result.ctxt_updates), 1)

        self.assertTrue(ctxt_handle in self._mdib.context_by_handle)

        descr = self._mdib.descriptions.NODETYPE.get(pm_qnames.NumericMetricDescriptor)[0]
        state = NumericMetricStateContainer(descr)
        with self._mdib.context_state_transaction() as mgr:
            self.assertRaises(ApiUsageError, mgr.add_state, state)

    def test_description_modification(self):
        """Verify that descriptor_transaction works as expected.

        - mdib_version is incremented
        - observable updated_descriptors_by_handle is updated
        - corresponding states for descriptor modifications are also updated
        - ApiUsageError is thrown if data of wrong kind is requested
        """
        mdib_version = self._mdib.mdib_version
        alert_condition_handle = self._mdib.descriptions.NODETYPE.get(pm_qnames.AlertConditionDescriptor)[0].Handle
        metric_handle = self._mdib.descriptions.NODETYPE.get(pm_qnames.NumericMetricDescriptor)[0].Handle
        operational_descr_handle = self._mdib.descriptions.NODETYPE.get(
            pm_qnames.SetAlertStateOperationDescriptor)[0].Handle
        component_descr_handle = self._mdib.descriptions.NODETYPE.get(pm_qnames.ChannelDescriptor)[0].Handle
        rt_descr_handle = self._mdib.descriptions.NODETYPE.get(pm_qnames.RealTimeSampleArrayMetricDescriptor)[0].Handle
        context_descr_handle = self._mdib.descriptions.NODETYPE.get(pm_qnames.PatientContextDescriptor)[0].Handle


        with self._mdib.descriptor_transaction() as mgr:
            # verify that updating descriptors of different kinds and accessing corresponding states works
            mgr.get_descriptor(alert_condition_handle)
            mgr.get_state(alert_condition_handle)
            mgr.get_descriptor(metric_handle)
            mgr.get_state(metric_handle)
            mgr.get_descriptor(operational_descr_handle)
            mgr.get_state(operational_descr_handle)
            mgr.get_descriptor(component_descr_handle)
            mgr.get_state(component_descr_handle)
            mgr.get_descriptor(rt_descr_handle)
            mgr.get_state(rt_descr_handle)
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)
        transaction_result = self._mdib.transaction
        self.assertEqual(len(transaction_result.metric_updates), 1)
        self.assertEqual(len(transaction_result.alert_updates), 1)
        self.assertEqual(len(transaction_result.op_updates), 1)
        self.assertEqual(len(transaction_result.comp_updates), 1)
        self.assertEqual(len(transaction_result.rt_updates), 1)
        self.assertEqual(len(transaction_result.descr_updated), 5)

        self.assertTrue(alert_condition_handle in self._mdib.updated_descriptors_by_handle)
        self.assertTrue(alert_condition_handle in self._mdib.alert_by_handle)
        self.assertTrue(metric_handle in self._mdib.updated_descriptors_by_handle)
        self.assertTrue(metric_handle in self._mdib.metrics_by_handle)
        self.assertTrue(operational_descr_handle in self._mdib.updated_descriptors_by_handle)
        self.assertTrue(operational_descr_handle in self._mdib.operation_by_handle)
        self.assertTrue(component_descr_handle in self._mdib.updated_descriptors_by_handle)
        self.assertTrue(component_descr_handle in self._mdib.component_by_handle)
        self.assertTrue(rt_descr_handle in self._mdib.updated_descriptors_by_handle)


        # verify that accessing a state for that the descriptor is not part of transaction is not allowed
        with self._mdib.descriptor_transaction() as mgr:
            mgr.get_descriptor(alert_condition_handle)
            self.assertRaises(ApiUsageError, mgr.get_state, metric_handle)
        self.assertEqual(mdib_version + 2, self._mdib.mdib_version)

        # verify that get_state for a context state raises an ApiUsageError
        with self._mdib.descriptor_transaction() as mgr:
            mgr.get_descriptor(context_descr_handle)
            self.assertRaises(ApiUsageError, mgr.get_state, context_descr_handle)

    def test_remove_add(self):
        """Verify that removing descriptors / states and adding them later again results in correct versions."""
        descriptors = {descr.Handle: descr.mk_copy() for descr in self._mdib.descriptions.objects}
        states = {state.DescriptorHandle: state.mk_copy() for state in self._mdib.states.objects}
        context_states = {state.Handle: state.mk_copy() for state in self._mdib.context_states.objects}

        # remove all root descriptors
        root_descr = self._mdib.descriptions.parent_handle.get(None)
        with self._mdib.descriptor_transaction() as mgr:
            for descr in root_descr:
                mgr.remove_descriptor(descr.Handle)

        self.assertEqual(0, len(self._mdib.descriptions.objects))
        self.assertEqual(0, len(self._mdib.states.objects))
        self.assertEqual(0, len(self._mdib.context_states.objects))

        with self._mdib.descriptor_transaction() as mgr:
            for descr in descriptors.values():
                mgr.add_descriptor(descr.mk_copy())
            for state in states.values():
                mgr.add_state(state.mk_copy())
            for state in context_states.values():
                mgr.add_state(state.mk_copy())

        current_descriptors = {descr.Handle: descr.mk_copy() for descr in self._mdib.descriptions.objects}

        # verify that all descriptors have incremented version
        for descr in self._mdib.descriptions.objects:
            self.assertEqual(descr.DescriptorVersion, current_descriptors[descr.Handle].DescriptorVersion)

        # verify that all states have incremented version
        for state in self._mdib.states.objects:
            self.assertEqual(state.DescriptorVersion, current_descriptors[state.DescriptorHandle].DescriptorVersion)
            self.assertEqual(state.StateVersion, states[state.DescriptorHandle].StateVersion + 1)

        # verify that all context states have incremented version
        for state in self._mdib.context_states.objects:
            self.assertEqual(state.DescriptorVersion, current_descriptors[state.DescriptorHandle].DescriptorVersion)
            self.assertEqual(state.StateVersion, context_states[state.Handle].StateVersion + 1)

        # verify transaction content is als correct
        transaction_result = self._mdib.transaction
        for descr in transaction_result.descr_created:
            self.assertEqual(descr.DescriptorVersion, current_descriptors[descr.Handle].DescriptorVersion)

        for state in transaction_result.all_states():
            self.assertEqual(state.DescriptorVersion, current_descriptors[state.DescriptorHandle].DescriptorVersion)



class TestEntityTransactions(unittest.TestCase):
    """Test all kinds of transactions for entity interface of ProviderMdib."""

    def setUp(self):
        self._mdib = ProviderMdib.from_mdib_file(mdib_file,
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
        old_ac_entity = self._mdib.entities.by_node_type(pm_qnames.AlertConditionDescriptor)[0]
        old_ac_entity.state.Presence = True
        with self._mdib.alert_state_transaction() as mgr:
            mgr.write_entity(old_ac_entity)
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)
        self.assertEqual(len(self._mdib.transaction.alert_updates), 1)  # this causes an EpisodicAlertReport

        new_ac_entity = self._mdib.entities.by_handle(old_ac_entity.handle)
        self.assertEqual(new_ac_entity.state.StateVersion, old_ac_entity.state.StateVersion + 1)

        metric_entities = self._mdib.entities.by_node_type(pm_qnames.NumericMetricDescriptor)
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
        old_metric_entity = self._mdib.entities.by_node_type(pm_qnames.NumericMetricDescriptor)[0]
        old_metric_entity.state.LifeTimePeriod = 2
        with self._mdib.metric_state_transaction() as mgr:
            mgr.write_entity(old_metric_entity)
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)
        self.assertEqual(len(self._mdib.transaction.metric_updates), 1)

        new_metric_entity = self._mdib.entities.by_handle(old_metric_entity.handle)
        self.assertEqual(new_metric_entity.state.StateVersion, old_metric_entity.state.StateVersion + 1)

        ac_entities = self._mdib.entities.by_node_type(pm_qnames.AlertConditionDescriptor)
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
        old_op_entity = self._mdib.entities.by_node_type(pm_qnames.SetAlertStateOperationDescriptor)[0]
        old_op_entity.state.OperationMode = pm_types.OperatingMode.DISABLED
        with self._mdib.operational_state_transaction() as mgr:
            mgr.write_entity(old_op_entity)
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)
        self.assertEqual(len(self._mdib.transaction.op_updates), 1)
        self.assertTrue(old_op_entity.handle in self._mdib.operation_by_handle)

        new_op_entity = self._mdib.entities.by_handle(old_op_entity.handle)
        self.assertEqual(new_op_entity.state.StateVersion, old_op_entity.state.StateVersion + 1)

        metric_entities = self._mdib.entities.by_node_type(pm_qnames.NumericMetricDescriptor)
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
        old_pat_entity = self._mdib.entities.by_node_type(pm_qnames.PatientContextDescriptor)[0]
        new_state = old_pat_entity.new_state()
        self.assertIsNotNone(new_state.Handle)
        new_state.CoreData.Givenname = 'foo'
        new_state.CoreData.Familyname = 'bar'
        context_handle = new_state.Handle

        with self._mdib.context_state_transaction() as mgr:
            mgr.write_entity(old_pat_entity, [context_handle])
        self.assertEqual(mdib_version + 1, self._mdib.mdib_version)
        self.assertEqual(len(self._mdib.transaction.ctxt_updates), 1)

        new_pat_entity = self._mdib.entities.by_handle(old_pat_entity.handle)
        self.assertEqual(new_pat_entity.states[context_handle].StateVersion,0)
        self.assertEqual(new_pat_entity.states[context_handle].CoreData.Givenname,'foo')
        self.assertEqual(new_pat_entity.states[context_handle].CoreData.Familyname,'bar')

        new_pat_entity.states[context_handle].CoreData.Familyname = 'foobar'

        with self._mdib.context_state_transaction() as mgr:
            mgr.write_entity(new_pat_entity, [context_handle])

        newest_pat_entity =  self._mdib.entities.by_handle(old_pat_entity.handle)
        self.assertEqual(newest_pat_entity.states[context_handle].StateVersion,1)
        self.assertEqual(newest_pat_entity.states[context_handle].CoreData.Familyname,'foobar')

        metric_entities = self._mdib.entities.by_node_type(pm_qnames.NumericMetricDescriptor)
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
        old_ac_entity = self._mdib.entities.by_node_type(pm_qnames.AlertConditionDescriptor)[0]
        old_metric_entity = self._mdib.entities.by_node_type(pm_qnames.NumericMetricDescriptor)[0]
        old_op_entity = self._mdib.entities.by_node_type(pm_qnames.SetAlertStateOperationDescriptor)[0]
        old_comp_entity = self._mdib.entities.by_node_type(pm_qnames.ChannelDescriptor)[0]
        old_rt_entity = self._mdib.entities.by_node_type(pm_qnames.RealTimeSampleArrayMetricDescriptor)[0]
        old_ctx_entity = self._mdib.entities.by_node_type(pm_qnames.PatientContextDescriptor)[0]

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
        for descr in  self._mdib.descriptions.objects:
            all_entities[descr.Handle] = self._mdib.entities.by_handle(descr.Handle) # get external representation

        root_entities = self._mdib.entities.by_parent_handle(None)
        with self._mdib.descriptor_transaction() as mgr:
            for ent in root_entities:
                mgr.remove_entity(ent)

        self.assertEqual(0, len(self._mdib.descriptions.objects))

        # add all entities again
        with self._mdib.descriptor_transaction() as mgr:
            mgr.write_entities(all_entities.values())

        # verify that the number of entities is the same as before
        self.assertEqual(len(all_entities), len(self._mdib.descriptions.objects))

        # verify that all descriptors and states have incremented version counters
        # for current_ent in self._mdib.internal_entities.values():
        for handle in all_entities:
            current_ent = self._mdib.entities.by_handle(handle)
            old_ent = all_entities[current_ent.handle]
            self.assertEqual(current_ent.descriptor.DescriptorVersion, old_ent.descriptor.DescriptorVersion + 1)
            if current_ent.is_multi_state:
                for state_handle, current_state in current_ent.states.items():
                    old_state = old_ent.states[state_handle]
                    self.assertEqual(current_state.StateVersion, old_state.StateVersion + 1)
                    self.assertEqual(current_state.DescriptorVersion, current_ent.descriptor.DescriptorVersion)
            else:
                self.assertEqual(current_ent.state.StateVersion, old_ent.state.StateVersion + 1)
                self.assertEqual(current_ent.state.DescriptorVersion, current_ent.descriptor.DescriptorVersion)
