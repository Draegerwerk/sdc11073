import unittest
import os
from sdc11073 import mdib
from sdc11073 import pmtypes
mdibFolder = os.path.dirname(__file__)

class TestMdib(unittest.TestCase):
    
    
    def test_selectDescriptors(self):
        
        deviceMdibContainer = mdib.DeviceMdibContainer.fromMdibFile(os.path.join(mdibFolder, '70041_MDIB_Final.xml'))
        # from looking at the mdib file I know how many elements the tested pathes shall return
        for path, expectedCount in [(('70041',), 1),
                                    (('70041', '69650'), 1),  # VMDs
                                    (('70041', '69650', '69651'), 1),  # Channels
                                    (('70041', '69650', '69651', '152464'), 4),  # Metrics
                                    ]:
            found = deviceMdibContainer.selectDescriptors(*path)
            self.assertEqual(len(found), expectedCount, msg='expect {} containers for path {}, found {}'.format(expectedCount, path, found))
            for f in found:
                self.assertEqual(f.codeId, path[-1])

    def test_mdib_tns(self):
        # verify that a mdib with participant model as default namespace can be handled.
        # if creation does not raise any exception, all should be fine.
        deviceMdibContainer = mdib.DeviceMdibContainer.fromMdibFile(os.path.join(os.path.dirname(__file__), 'mdib_tns.xml'))
        self.assertTrue(deviceMdibContainer is not None)

    def test_default_coding_system_change(self):
        default_coding_system = pmtypes.DefaultCodingSystem
        other_default_coding_system = 'urn:oid:1.2.3.4.5.6.7'
        try:
            deviceMdibContainer = mdib.DeviceMdibContainer.fromMdibFile(
                os.path.join(os.path.dirname(__file__), 'mdib_tns.xml'))
            mds = deviceMdibContainer.descriptions.handle.getOne('mds0')
            self.assertEqual(mds.Type.codingSystem, default_coding_system)
            # now change constant and verify that coding system did also change
            pmtypes.DefaultCodingSystem = other_default_coding_system
            self.assertEqual(mds.Type.codingSystem, other_default_coding_system)
        finally:
            pmtypes.DefaultCodingSystem = default_coding_system

def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestMdib)
        
        
if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
