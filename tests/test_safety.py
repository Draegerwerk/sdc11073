import unittest

from lxml import etree as etree_

from sdc11073 import safety
from sdc11073.namespaces import default_ns_helper as ns_hlp  #  Prefixes
#from sdc11073.namespaces import nsmap

# a wsdl taken from safety example
safety_mdib_response = '''<?xml version="1.0" encoding="UTF-8"?>
<msg:GetMdibResponse xmlns:dom="{dom}" xmlns:ext="{ext}" xmlns:msg="{msg}" xmlns:si="{si}" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://message-model-uri/15/04 ../BICEPS_MessageModel.xsd http://safety-information-uri/15/08 ../SafetyInformation.xsd" dom:MDIBVersion="4294967295">
    <msg:Mdib dom:MDIBVersion="4294967295">
        <dom:MdDescription DescriptionVersion="4294967295">
            <dom:Mds Handle="mds" DescriptorVersion="4294967295" IntendedUse="MedB" xsi:type="dom:HydraMDSDescriptor">
                <dom:Context/>
                <dom:Sco Handle="sco">
                    <dom:Operation Handle="op1" OperationTarget="metric1" xsi:type="dom:SetStringOperationDescriptor">
                        <ext:Extension>
                          <si:SafetyReq>
                            <si:DualChannelDef Algorithm="si:B64SHA1" Transform="si:xml-exc-c14n">
                                <si:Selector Id="dcSel1">/msg:SetString/msg:RequestedStringValue/text()</si:Selector>
                                <si:Selector Id="dcSel2">/msg:SetString/msg:OperationHandleRef/text()</si:Selector>
                            </si:DualChannelDef>
                            <si:SafetyContextDef>
                                <si:Selector Id="scSel1">/dom:MDDescription/dom:MDS[@Handle='mds']/dom:VMD[@Handle='vmd1']/dom:Channel[@Handle='chan1']/dom:Metric[@Handle='metric1']/dom:Unit/dom:CodeId/text()</si:Selector>
                                <si:Selector Id="scSel2">/dom:MDState/dom:State[@DescriptorHandle='metric1']/dom:ObservedValue/@Value</si:Selector>
                            </si:SafetyContextDef>
                        </si:SafetyReq>
                        </ext:Extension>
                    </dom:Operation>
                </dom:Sco>
                <dom:Vmd Handle="vmd1">
                    <dom:Channel Handle="chan1">
                        <dom:Metric Handle="metric1" xsi:type="dom:StringMetricDescriptor" DescriptorVersion="1">
                            <dom:Unit>
                                <dom:CodingSystemId>urn:oid:1.3.6.1.4.1.19376.1.6.7.1</dom:CodingSystemId>
                                <dom:CodeId>MDC_DIMLESS</dom:CodeId>
                            </dom:Unit>
                            <dom:MetricCategory>Set</dom:MetricCategory>
                            <dom:Availability>Intr</dom:Availability>
                        </dom:Metric>
                    </dom:Channel>
                </dom:Vmd>
            </dom:Mds>
        </dom:MdDescription>
        <dom:MdState>
            <dom:State DescriptorHandle="metric1" xsi:type="dom:StringMetricState">
                <dom:ObservedValue Value="NEXT_VALUE">
                    <dom:MeasurementState Validity="Calib"/>
                </dom:ObservedValue>
            </dom:State>
        </dom:MdState>
    </msg:Mdib>
</msg:GetMdibResponse>
'''.format(msg=ns_hlp.MSG.prefix, ext=ns_hlp.EXT.prefix, dom=ns_hlp.PM.prefix, si=ns_hlp.MDPWS.prefix)


def parseXMLString(xmlString, **kwargs):
    parser = etree_.ETCompatXMLParser()
    doc = etree_.fromstring(xmlString, parser=parser, **kwargs)
    return doc


class TestSafety(unittest.TestCase):

    def test_SafetyInfoHeader(self):
        # verify that SafetyInfoHeader creates a correct safety info header node
        dual_channel = {'dcSel1': 'TEST_VALUE', 'dcSel2': '4'}
        safety_context = {'scSel1': 'ORIGINAL_TEST_VALUE', 'scSel2': '3'}
        for algo in safety.sha1, safety.base64_sha1:  # test with both encryption algorithms that are provided in module
            si = safety.SafetyInfoHeader(ns_hlp, dual_channel, safety_context, algo)
            root_node = etree_.Element('bla')  # name of root does not matter
            root_node.append(si.as_etree_node())
            # read relevant values from node anv verify that they are sha1 encoded (default encoding)
            # paths are same as in above xml
            dc_sel1 = root_node.xpath(
                "/bla/{si}:SafetyInfo/{si}:DualChannel/{si}:DcValue[@ReferencedSelector='dcSel1']".format(
                    si=ns_hlp.MDPWS.prefix), namespaces=ns_hlp.ns_map)
            dc_sel2 = root_node.xpath(
                "/bla/{si}:SafetyInfo/{si}:DualChannel/{si}:DcValue[@ReferencedSelector='dcSel2']".format(
                    si=ns_hlp.MDPWS.prefix), namespaces=ns_hlp.ns_map)

            sc_sel1 = root_node.xpath(
                '/bla/{si}:SafetyInfo/{si}:SafetyContext/{si}:CtxtValue[@ReferencedSelector="scSel1"]'.format(
                    si=ns_hlp.MDPWS.prefix), namespaces=ns_hlp.ns_map)
            sc_sel2 = root_node.xpath(
                '/bla/{si}:SafetyInfo/{si}:SafetyContext/{si}:CtxtValue[@ReferencedSelector="scSel2"]'.format(
                    si=ns_hlp.MDPWS.prefix), namespaces=ns_hlp.ns_map)

            self.assertEqual(dc_sel1[0].text, algo('TEST_VALUE'))
            self.assertEqual(dc_sel2[0].text, algo('4'))
            self.assertEqual(sc_sel1[0].text, 'ORIGINAL_TEST_VALUE')
            self.assertEqual(sc_sel2[0].text, '3')


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestSafety)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
