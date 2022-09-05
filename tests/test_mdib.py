import unittest
import os
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
            self.assertEqual(len(found), expectedCount, msg='expect {} containers for path {}, found {}'.format(expectedCount, path, found))
            for f in found:
                self.assertEqual(f.code_id, path[-1])

    def test_mdib_tns(self):
        # verify that a mdib with participant model as default namespace can be handled.
        # if creation does not raise any exception, all should be fine.
        device_mdib_container = DeviceMdibContainer.from_mdib_file(os.path.join(os.path.dirname(__file__), 'mdib_tns.xml'))
        self.assertTrue(device_mdib_container is not None)

    def test_get_metric_descriptor_by_code(self):
        device_mdib_container = DeviceMdibContainer.from_mdib_file(os.path.join(os.path.dirname(__file__), 'mdib_tns.xml'))
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

def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestMdib)
        
        
if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
