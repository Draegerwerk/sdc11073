import os
import unittest
from dataclasses import dataclass
from lxml.etree import QName
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



    def test_activate_operation_argument(self):
        """Test that pm:ActivateOperationDescriptor/pm:argument/pm:Arg is handled correctly.

        QName as node text is beyond what xml libraries can handle automatically,
        it must be handled specifically in sdc11073 code.
        """

        @dataclass
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
            mdib_container = ProviderMdib.from_string(mdib_text)
            mdib_node, mdib_version_group = mdib_container.reconstruct_mdib_with_context_states()
            arg_nodes = mdib_node.xpath('//*/pm:Arg', namespaces={'pm': "http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"})
            arg_node = arg_nodes[0]
            prefix = arg_node.text.split(':')[0]
            self.assertTrue(prefix in arg_node.nsmap)
            self.assertEqual(test_data.expected_qname.namespace, arg_node.nsmap[prefix])
            self.assertEqual(test_data.expected_qname.localname, arg_node.text.split(':')[1])