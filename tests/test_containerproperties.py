import dataclasses
import datetime
import unittest
from unittest import mock

from lxml import etree

import sdc11073.mdib.containerproperties as containerproperties
from sdc11073.definitions_sdc import SDC_v1_Definitions
from sdc11073.mdib import clientmdib
from sdc11073.mdib import devicemdib
from sdc11073.mdib import msgreader
from sdc11073.mdib.descriptorcontainers import ClockDescriptorContainer
from sdc11073.namespaces import DocNamespaceHelper
from sdc11073.namespaces import domTag
from sdc11073.namespaces import msgTag

# pylint: disable=protected-access

DoB = containerproperties.DateOfBirthProperty


class TestContainerproperties(unittest.TestCase):

    def test_DateOfBirthRegEx(self):
        result = DoB.mk_value_object('2003-06-30')
        self.assertEqual(result, datetime.date(2003, 6, 30))

        for text in ('foo', '0000-06-30', '01-00-01', '01-01-00'):  # several invalid strings
            result = DoB.mk_value_object(text)
            self.assertTrue(result is None, msg='result of {} should be None, but it is {}'.format(text, result))

        result = DoB.mk_value_object('2003-06-30T14:53:12.4')
        self.assertEqual(result, datetime.datetime(2003, 6, 30, 14, 53, 12, 400000))
        self.assertEqual(result.tzinfo, None)

        # add time zone UTC
        result = DoB.mk_value_object('2003-06-30T15:53:12Z')
        self.assertEqual(result.second, 12)
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.tzinfo.utcoffset(0), datetime.timedelta(0))

        # add time zone UTC
        result = DoB.mk_value_object('2003-06-30T15:53:12.4Z')
        self.assertEqual(result.second, 12)
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.tzinfo.utcoffset(0), datetime.timedelta(0))

        # add time zone +6hours
        result = DoB.mk_value_object('2003-06-30T15:53:12.4+6:02')
        self.assertEqual(result.second, 12)
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.tzinfo.utcoffset(0), datetime.timedelta(minutes=60 * 6 + 2))

        # add time zone -3hours
        result = DoB.mk_value_object('2003-06-30T15:53:12.4-03:01')
        self.assertEqual(result.second, 12)
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.tzinfo.utcoffset(0), datetime.timedelta(minutes=(30 * 6 + 1) * -1))

    def test_DateOfBirth_toString(self):
        datestring = DoB._mk_datestring(datetime.date(2004, 3, 6))
        self.assertEqual(datestring, '2004-03-06')

        datestring = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16))
        self.assertEqual(datestring, '2004-03-06T14:15:16')

        datestring = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 4, 5, 6))
        self.assertEqual(datestring, '2004-03-06T04:05:06')  # verify leading zeros in date and time

        datestring = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000))
        self.assertEqual(datestring, '2004-03-06T14:15:16.7')

        datestring = DoB._mk_datestring(
            datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=containerproperties.UTC(0, 'UTC')))
        self.assertEqual(datestring, '2004-03-06T14:15:16.7Z')

        datestring = DoB._mk_datestring(
            datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=containerproperties.UTC(180, 'UTC+1')))
        self.assertEqual(datestring, '2004-03-06T14:15:16.7+03:00')

        datestring = DoB._mk_datestring(
            datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=containerproperties.UTC(-120, 'UTC+1')))
        self.assertEqual(datestring, '2004-03-06T14:15:16.7-02:00')

        datestring = DoB._mk_datestring(
            datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=containerproperties.UTC(181, 'UTC+1')))
        self.assertEqual(datestring, '2004-03-06T14:15:16.7+03:01')

        datestring = DoB._mk_datestring(
            datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=containerproperties.UTC(-121, 'UTC+1')))
        self.assertEqual(datestring, '2004-03-06T14:15:16.7-02:01')

    def test_duration(self):
        # Duration strings shall be kept as they are available in node,
        # even if decimal converter would convert to a different string
        ns_mapper = DocNamespaceHelper()
        dc = ClockDescriptorContainer(nsmapper=ns_mapper,
                                      nodeName=domTag('MyDescriptor'),
                                      handle='123',
                                      parentHandle='456',
                                      )
        # create etree node and set Resolution to something that would be changed
        node = dc.mkNode()
        node.attrib['Resolution'] = 'PT77S'

        dc2 = ClockDescriptorContainer.fromNode(nsmapper=ns_mapper,
                                                node=node,
                                                parentHandle='467')
        node2 = dc2.mkNode()
        self.assertEqual(node2.attrib['Resolution'], 'PT77S')

    def test_activate_operation_argument(self):
        """Test that pm:ActivateOperationDescriptor/pm:argument/pm:Arg is handled correctly
        because its value is a QName"""
        @dataclasses.dataclass
        class TestData:
            mdib_text: str
            expected_qname: etree.QName

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
        expected_qname_xsd = etree.QName("http://www.w3.org/2001/XMLSchema", "duration")
        expected_qname_any_uri = etree.QName("urn:oid:1.23.3.123.2", "duration")

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
            mdib_container = devicemdib.DeviceMdibContainer.fromString(mdib_text)
            mdib_node, mdib_version_group = mdib_container.reconstructMdibWithContextStates()
            getMdibResponseNode = etree.Element(msgTag('GetMdibResponse'), nsmap=mdib_node.nsmap)
            mdib_version_group.update_node(getMdibResponseNode)
            getMdibResponseNode.append(mdib_node)
            mdib_container.sdc_definitions.xml_validator.assertValid(getMdibResponseNode)

            # read msg:GetMdibResponse as an sdc11073 client would do
            # and verify that pm:Argument/pm:Arg data is still valid

            # Preparation
            sdc_client = mock.Mock()
            sdc_client.sdc_definitions = SDC_v1_Definitions
            sdc_client.log_prefix = ""
            client_mdib = clientmdib.ClientMdibContainer(sdc_client)
            tmp_msg_reader = msgreader.MessageReader(client_mdib)

            # parse getMdibResponseNode to container
            descriptors = tmp_msg_reader.readMdDescription(getMdibResponseNode)
            op_descr = [descr for descr in descriptors if descr.isOperationalDescriptor][0]
            tmp_arg = op_descr.Argument[0].Arg
            self.assertEqual(tmp_arg.localname, test_data.expected_qname.localname)
            self.assertEqual(tmp_arg.namespace, test_data.expected_qname.namespace)
            self.assertEqual(tmp_arg.text, test_data.expected_qname.text)
            self.assertIn(tmp_arg.namespace, tmp_arg.nsmap.values())


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestContainerproperties)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
