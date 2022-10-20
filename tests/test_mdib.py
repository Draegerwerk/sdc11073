import os
import unittest

from sdc11073 import pm_qnames as pm
from sdc11073.exceptions import ApiUsageError
from sdc11073.mdib import DeviceMdibContainer
from sdc11073.pmtypes import Coding

mdibFolder = os.path.dirname(__file__)


class TestMdib(unittest.TestCase):

    def test_selectDescriptors(self):

        device_mdib_container = DeviceMdibContainer.from_mdib_file(os.path.join(mdibFolder, '70041_MDIB_Final.xml'))
        # from looking at the mdib file I know how many elements the tested pathes shall return
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
        device_mdib_container = DeviceMdibContainer.from_mdib_file(
            os.path.join(os.path.dirname(__file__), 'mdib_tns.xml'))
        self.assertTrue(device_mdib_container is not None)

    def test_get_metric_descriptor_by_code(self):
        device_mdib_container = DeviceMdibContainer.from_mdib_file(
            os.path.join(os.path.dirname(__file__), 'mdib_tns.xml'))
        metric_container = device_mdib_container.get_metric_descriptor_by_code(vmd_code=Coding("130536"),
                                                                               channel_code=Coding("130637"),
                                                                               metric_code=Coding("196174"))
        self.assertIsNotNone(metric_container)
        metric_container = device_mdib_container.get_metric_descriptor_by_code(vmd_code=Coding("xxxxx"),
                                                                               channel_code=Coding("130637"),
                                                                               metric_code=Coding("196174"))
        self.assertIsNone(metric_container)
        metric_container = device_mdib_container.get_metric_descriptor_by_code(vmd_code=Coding("130536"),
                                                                               channel_code=Coding("130637"),
                                                                               metric_code=Coding("xxxxx"))
        self.assertIsNone(metric_container)


class TestMdibTransaction(unittest.TestCase):

    def setUp(self):
        self.mdib = DeviceMdibContainer.from_mdib_file(os.path.join(os.path.dirname(__file__), 'mdib_tns.xml'))

    def test_create_descriptor(self):
        with self.mdib.transaction_manager() as mgr:
            descriptor_container = self.mdib.data_model.mk_descriptor_container(
                pm.NumericMetricDescriptor, handle="testHandle", parent_handle="ch0.vmd0")
            state = self.mdib.data_model.mk_state_container(descriptor_container)
            mgr.add_descriptor(descriptor_container, state_container=state)
        tr = self.mdib.transaction
        self.assertEqual(1, len(tr.descr_created))
        self.assertEqual(1, len(tr.descr_updated))
        self.assertEqual(2, len(tr.all_states()))

    def test_create_descriptor_without_state(self):
        with self.mdib.transaction_manager() as mgr:  # now without state
            descriptor_container = self.mdib.data_model.mk_descriptor_container(
                pm.NumericMetricDescriptor, handle="testHandle", parent_handle="ch0.vmd0")
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
            self.assertRaises(ValueError, mgr.get_state, 'numeric.ch0.vmd0')
            self.assertRaises(ApiUsageError, mgr.get_state, 'numeric.ch1.vmd0')

    def test_update_descriptor_wrong_state(self):
        with self.mdib.transaction_manager() as mgr:
            metric_descriptor = mgr.get_descriptor('numeric.ch0.vmd0')
            metric_descriptor.DeterminationPeriod = 29.0
            self.assertRaises(ApiUsageError, mgr.get_state, 'numeric.ch1.vmd0')

    def test_get_mixed_states(self):
        with self.mdib.transaction_manager() as mgr:
            state = mgr.get_state('numeric.ch0.vmd0')
            self.assertRaises(ApiUsageError, mgr.get_state, 'ch0.vmd0')


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestMdib)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
