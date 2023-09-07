import os
import unittest

from sdc11073.xml_types import pm_qnames as pm
from sdc11073.exceptions import ApiUsageError
from sdc11073.mdib import ProviderMdib
from sdc11073.xml_types.pm_types import Coding
from sdc11073 import definitions_sdc

mdib_folder = os.path.dirname(__file__)

#mdib_tns_path = os.path.join(mdib_folder, 'mdib_tns.xml')
mdib_tns_path = os.path.join(mdib_folder, 'mdib_two_mds.xml')
mdib_70041_path = os.path.join(mdib_folder, '70041_MDIB_Final.xml')

class TestMdib(unittest.TestCase):

    def test_select_descriptors(self):

        device_mdib_container = ProviderMdib.from_mdib_file(mdib_70041_path,
                                                            protocol_definition=definitions_sdc.SdcV1Definitions)
        # from looking at the mdib file I know how many elements the tested paths shall return
        for path, expectedCount in [(('70041',), 1),
                                    (('70041', '69650'), 1),  # VMDs
                                    (('70041', '69650', '69651'), 1),  # Channels
                                    (('70041', '69650', '69651', '152464'), 4),  # Metrics
                                    ]:
            found = device_mdib_container.select_descriptors(*path)
            self.assertEqual(len(found), expectedCount,
                             msg='expect {} containers for path {}, found {}'.format(expectedCount, path, found))
            for f in found:
                self.assertEqual(f.code_id, path[-1])

    def test_mdib_tns(self):
        # verify that a mdib with participant model as default namespace can be handled.
        # if creation does not raise any exception, all should be fine.
        device_mdib_container = ProviderMdib.from_mdib_file(
            os.path.join(os.path.dirname(__file__), 'mdib_tns.xml'),
        protocol_definition=definitions_sdc.SdcV1Definitions)
        self.assertTrue(device_mdib_container is not None)

    def test_get_metric_descriptor_by_code(self):
        device_mdib_container = ProviderMdib.from_mdib_file(mdib_tns_path,
                                                            protocol_definition=definitions_sdc.SdcV1Definitions)
        metric_container = device_mdib_container.get_metric_descriptor_by_code(vmd_code=Coding("130536"),
                                                                               channel_code=Coding("130637"),
                                                                               metric_code=Coding("196174"))
        self.assertIsNotNone(metric_container)
        metric_container = device_mdib_container.get_metric_descriptor_by_code(vmd_code=Coding("98765"),
                                                                               channel_code=Coding("130637"),
                                                                               metric_code=Coding("196174"))
        self.assertIsNone(metric_container)
        metric_container = device_mdib_container.get_metric_descriptor_by_code(vmd_code=Coding("130536"),
                                                                               channel_code=Coding("130637"),
                                                                               metric_code=Coding("98765"))
        self.assertIsNone(metric_container)


class TestMdibTransaction(unittest.TestCase):

    def setUp(self):
        self.mdib = ProviderMdib.from_mdib_file(mdib_tns_path,
                                                protocol_definition=definitions_sdc.SdcV1Definitions)

    def test_create_delete_descriptor(self):
        with self.mdib.transaction_manager() as mgr:
            parent_descriptor = self.mdib.descriptions.handle.get_one("ch0.vmd0")
            descriptor_container = self.mdib.data_model.mk_descriptor_container(
                pm.NumericMetricDescriptor, handle="testHandle", parent_descriptor=parent_descriptor)
            state = self.mdib.data_model.mk_state_container(descriptor_container)
            mgr.add_descriptor(descriptor_container, state_container=state)
        tr = self.mdib.transaction
        self.assertEqual(1, len(tr.descr_created))
        self.assertEqual(1, len(tr.descr_updated))
        self.assertEqual(2, len(tr.all_states()))
        descr = self.mdib.descriptions.handle.get_one("testHandle", allow_none=True)
        state = self.mdib.states.descriptor_handle.get_one("testHandle", allow_none=True)
        self.assertIsNotNone(descr)
        self.assertIsNotNone(state)

        with self.mdib.transaction_manager() as mgr:
            mgr.remove_descriptor("testHandle")
        tr = self.mdib.transaction
        self.assertEqual(0, len(tr.descr_created))
        self.assertEqual(1, len(tr.descr_updated))
        self.assertEqual(1, len(tr.all_states()))

        descr = self.mdib.descriptions.handle.get_one("testHandle", allow_none=True)
        state = self.mdib.states.descriptor_handle.get_one("testHandle", allow_none=True)
        self.assertIsNone(descr)
        self.assertIsNone(state)

    def test_create_descriptor_without_state(self):
        with self.mdib.transaction_manager() as mgr:  # now without state
            parent_descriptor = self.mdib.descriptions.handle.get_one("ch0.vmd0")
            descriptor_container = self.mdib.data_model.mk_descriptor_container(
                pm.NumericMetricDescriptor, handle="testHandle", parent_descriptor=parent_descriptor)
            mgr.add_descriptor(descriptor_container)
        tr = self.mdib.transaction
        self.assertEqual(1, len(tr.descr_created))
        self.assertEqual(1, len(tr.descr_updated))
        self.assertEqual(1, len(tr.all_states()))

    def test_update_descriptor_get_twice(self):
        with self.mdib.transaction_manager() as mgr:
            metric_descriptor = mgr.get_descriptor('numeric.ch0.vmd0')
            metric_descriptor.DeterminationPeriod = 29.0
            state = mgr.get_state('numeric.ch0.vmd0')
            self.assertEqual(state.DescriptorHandle, 'numeric.ch0.vmd0')
            self.assertRaises(ValueError, mgr.get_state, 'numeric.ch0.vmd0')  # second get_state call
            # next call failed due to a previous error
            self.assertRaises(ApiUsageError, mgr.get_state, 'numeric.ch1.vmd0')

    def test_update_descriptor_wrong_state(self):
        with self.mdib.transaction_manager() as mgr:
            metric_descriptor = mgr.get_descriptor('numeric.ch0.vmd0')
            metric_descriptor.DeterminationPeriod = 29.0
            self.assertRaises(ApiUsageError, mgr.get_state, 'numeric.ch1.vmd0')

    def test_get_mixed_states(self):
        with self.mdib.transaction_manager() as mgr:
            state = mgr.get_state('numeric.ch0.vmd0')
            self.assertEqual(state.DescriptorHandle, 'numeric.ch0.vmd0')
            self.assertRaises(ApiUsageError, mgr.get_state, 'ch0.vmd0')
