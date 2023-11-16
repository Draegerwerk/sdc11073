import os
import unittest
import dataclasses
from lxml.etree import QName
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
            self.assertEqual(len(found), expectedCount,
                             msg='expect {} containers for path {}, found {}'.format(expectedCount, path, found))
            for f in found:
                self.assertEqual(f.codeId, path[-1])

    def test_mdib_tns(self):
        # verify that a mdib with participant model as default namespace can be handled.
        # if creation does not raise any exception, all should be fine.
        deviceMdibContainer = mdib.DeviceMdibContainer.fromMdibFile(
            os.path.join(os.path.dirname(__file__), 'mdib_tns.xml'))
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

    def test_get_descriptor_by_code(self):
        deviceMdibContainer = mdib.DeviceMdibContainer.fromMdibFile(
            os.path.join(os.path.dirname(__file__), 'mdib_tns.xml'))
        # add a translation to a descriptor so that it can be tested
        handle = 'numeric.ch0.vmd0'
        vmd_type = pmtypes.CodedValue('130536')
        channel_type = pmtypes.CodedValue('130637')
        metric_type = pmtypes.CodedValue('196174')
        descriptor = deviceMdibContainer.descriptions.handle.getOne(handle)
        descriptor.Type.Translation.append(pmtypes.T_Translation('some_code', 'some_coding_system'))
        found1 = deviceMdibContainer.getDescriptorByCode(vmd_type, channel_type, metric_type)
        self.assertIsNotNone(found1)
        self.assertEqual(handle, found1.Handle)
        found2 = deviceMdibContainer.getDescriptorByCode(
            vmd_type, channel_type, pmtypes.CodedValue('some_code', 'some_coding_system'))
        self.assertIsNotNone(found2)
        self.assertEqual(handle, found2.Handle)

    def test_activate_operation_argument(self):
        """Test that pm:ActivateOperationDescriptor/pm:argument/pm:Arg is handled correctly
        because its value is a QName"""

        @dataclasses.dataclass
        class TestData:
            mdib_text: str
            expected_qname: QName

        mdib_dummy = """<msg:GetMdibResponse 
                        xmlns:msg="http://standards.ieee.org/downloads/11073/11073-10207-2017/message"
                         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                         {0}
                         xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                         xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                         xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension"
                         MdibVersion="174" SequenceId="urn:uuid:6f4ff7de-6809-4883-9938-54dd4f6f9173">
                        <msg:Mdib MdibVersion="174" SequenceId="urn:uuid:6f4ff7de-6809-4883-9938-54dd4f6f9173">
                            <pm:MdDescription DescriptionVersion="8">
                                <pm:Mds Handle="mds0" DescriptorVersion="5" SafetyClassification="MedA">
                                    <pm:Sco Handle="Sco.mds0" DescriptorVersion="2">
                                        <pm:Operation {1} xsi:type="pm:ActivateOperationDescriptor" Handle="SVO.38.3569" DescriptorVersion="2"
                                                      SafetyClassification="MedA" OperationTarget="3569" MaxTimeToFinish="PT00H00M02S"
                                                      Retriggerable="false">
                                            <pm:Type Code="193821">
                                                <pm:ConceptDescription Lang="en-US">An operation to cancel global all audio pause
                                                </pm:ConceptDescription>
                                            </pm:Type>
                                            <pm:Argument>
                                                <pm:ArgName Code="codeForArgumentName"></pm:ArgName>
                                                <pm:Arg {2}>{3}duration</pm:Arg>
                                            </pm:Argument>
                                        </pm:Operation>
                                    </pm:Sco>
                                    <pm:SystemContext Handle="SC.mds0" DescriptorVersion="2">
                                        <ext:Extension>
                                        </ext:Extension>
                                        <pm:PatientContext Handle="PC.mds0" DescriptorVersion="2">
                                        </pm:PatientContext>
                                        <pm:LocationContext Handle="LC.mds0" DescriptorVersion="2">
                                        </pm:LocationContext>
                                    </pm:SystemContext>
                                </pm:Mds>
                            </pm:MdDescription>
                        </msg:Mdib>
                    </msg:GetMdibResponse>"""

        my_prefix = "my"
        xsd_prefix = "xsd"
        delaration = 'xmlns:{0}="http://www.w3.org/2001/XMLSchema"'
        delaration_any_uri = 'xmlns:{0}="urn:oid:1.23.3.123.2"'
        expected_qname_xsd = QName("http://www.w3.org/2001/XMLSchema", "duration")
        expected_qname_any_uri = QName("urn:oid:1.23.3.123.2", "duration")

        mdibs = [TestData(mdib_text=mdib_dummy.format('', '', delaration.format(my_prefix), f"{my_prefix}:"),
                          expected_qname=expected_qname_xsd),
                 TestData(mdib_text=mdib_dummy.format('', delaration.format(my_prefix), '', f"{my_prefix}:"),
                          expected_qname=expected_qname_xsd),
                 TestData(mdib_text=mdib_dummy.format(delaration.format(my_prefix), '', '', f"{my_prefix}:"),
                          expected_qname=expected_qname_xsd),
                 TestData(mdib_text=mdib_dummy.format('', '', delaration.format(xsd_prefix), f"{xsd_prefix}:"),
                          expected_qname=expected_qname_xsd),
                 TestData(mdib_text=mdib_dummy.format('', '', 'xmlns="http://www.w3.org/2001/XMLSchema"', ''),
                          expected_qname=expected_qname_xsd),
                 TestData(mdib_text=mdib_dummy.format('', '', delaration_any_uri.format(xsd_prefix), f"{xsd_prefix}:"),
                          expected_qname=expected_qname_any_uri),
                 TestData(mdib_text=mdib_dummy.format('', delaration_any_uri.format(xsd_prefix), '', f"{xsd_prefix}:"),
                          expected_qname=expected_qname_any_uri)]

        for test_data in mdibs:
            # parse mdib data into container and reconstruct mdib data back to a msg:GetMdibResponse
            # so that it can be validated by xml schema validator
            mdib_text = test_data.mdib_text.encode('utf-8')
            mdib_container = mdib.DeviceMdibContainer.fromString(mdib_text)
            mdib_node, mdib_version_group = mdib_container.reconstructMdibWithContextStates()
            arg_nodes = mdib_node.xpath('//*/pm:Arg', namespaces={'pm': "__BICEPS_ParticipantModel__"})
            arg_node = arg_nodes[0]
            prefix = arg_node.text.split(':')[0]
            self.assertTrue('msg' in arg_node.nsmap)
            self.assertTrue(prefix in arg_node.nsmap)


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestMdib)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
