import unittest
import os
from sdc11073 import mdib
from sdc11073 import pmtypes
from lxml import etree as etree_

from sdc11073.mdib import ClientMdibContainer

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

    def test_copy_report_node(self):
        def test_xml(raw_xml: bytes):
            body = etree_.fromstring(raw_xml)[1]
            for report in body:
                new_report = ClientMdibContainer._copy_report_node(report)
                assert new_report.nsmap == report.nsmap
                assert etree_.tostring(new_report) == etree_.tostring(report)

        test_xml(b"""<?xml version='1.0' encoding='UTF-8'?>
<s12:Envelope xmlns:wse="http://schemas.xmlsoap.org/ws/2004/08/eventing"
              xmlns:wsa="http://www.w3.org/2005/08/addressing"
              xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
              xmlns:msg="http://standards.ieee.org/downloads/11073/11073-10207-2017/message"
              xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
              xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension"
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <s12:Header>
        <wsa:To s12:mustUnderstand="true">https://127.0.0.1:51341/</wsa:To>
        <wsa:Action s12:mustUnderstand="true">
            http://standards.ieee.org/downloads/11073/11073-20701-2018/StateEventService/EpisodicAlertReport
        </wsa:Action>
        <wsa:MessageID>urn:uuid:095ea71f-d5cb-4a6f-a12b-19a84933efef</wsa:MessageID>
        <ns0:MyClIdentifier xmlns:ns0="http.local.com" wsa:IsReferenceParameter="true">
            urn:uuid:75793341-2aa2-4a33-adfe-c158a0ad2982
        </ns0:MyClIdentifier>
    </s12:Header>
    <s12:Body>
        <msg:EpisodicAlertReport MdibVersion="10"
                                 SequenceId="urn:uuid:2d6bfcf5-a29c-42e4-99e3-99ce4a4e0233">
            <msg:ReportPart>
                <msg:AlertState xsi:type="pm:AlertSystemState" DescriptorVersion="0" StateVersion="7"
                                ActivationState="On" LastSelfCheck="1688025072995" SelfCheckCount="8"
                                PresentPhysiologicalAlarmConditions="" PresentTechnicalAlarmConditions=""
                                DescriptorHandle="ASYS0"/>
            </msg:ReportPart>
        </msg:EpisodicAlertReport>
    </s12:Body>
</s12:Envelope>""")

        test_xml(b"""<?xml version='1.0' encoding='UTF-8'?>
<s12:Envelope xmlns:wse="http://schemas.xmlsoap.org/ws/2004/08/eventing"
              xmlns:wsa="http://www.w3.org/2005/08/addressing"
              xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
              xmlns:msg="http://standards.ieee.org/downloads/11073/11073-10207-2017/message"
              xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
              xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension"
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <s12:Header>
        <wsa:To s12:mustUnderstand="true">https://127.0.0.1:51341/</wsa:To>
        <wsa:Action s12:mustUnderstand="true">
            http://standards.ieee.org/downloads/11073/11073-20701-2018/StateEventService/EpisodicAlertReport
        </wsa:Action>
        <wsa:MessageID>urn:uuid:095ea71f-d5cb-4a6f-a12b-19a84933efef</wsa:MessageID>
        <ns0:MyClIdentifier xmlns:ns0="http.local.com" wsa:IsReferenceParameter="true">
            urn:uuid:75793341-2aa2-4a33-adfe-c158a0ad2982
        </ns0:MyClIdentifier>
    </s12:Header>
    <s12:Body>
        <msg:EpisodicAlertReport MdibVersion="10"
                                 SequenceId="urn:uuid:2d6bfcf5-a29c-42e4-99e3-99ce4a4e0233">
            <msg:ReportPart>
                <msg:AlertState xsi:type="pm:AlertSystemState" DescriptorVersion="0" StateVersion="7"
                                ActivationState="On" LastSelfCheck="1688025072995" SelfCheckCount="8"
                                PresentPhysiologicalAlarmConditions="" PresentTechnicalAlarmConditions=""
                                DescriptorHandle="ASYS0"/>
            </msg:ReportPart>
        </msg:EpisodicAlertReport>
        <msg:EpisodicMetricReport MdibVersion="1180245"
                                  SequenceId="urn:uuid:5cdce523-d532-4d36-b5c1-42bf2fa8e3b0" InstanceId="0">
            <msg:ReportPart>
                <msg:SourceMds>mds0</msg:SourceMds>
                <msg:MetricState xsi:type="pm:NumericMetricState" StateVersion="21894"
                    DescriptorHandle="numeric.ch1.vmd0" DescriptorVersion="0">
                    <pm:MetricValue Value="21894" DeterminationTime="1688126346037">
                        <pm:MetricQuality Validity="Vld" Mode="Demo" />
                    </pm:MetricValue>
                </msg:MetricState>
            </msg:ReportPart>
        </msg:EpisodicMetricReport>
    </s12:Body>
</s12:Envelope>""")

        test_xml(b"""<?xml version='1.0' encoding='UTF-8'?>
<s12:Envelope xmlns:wse="http://schemas.xmlsoap.org/ws/2004/08/eventing"
              xmlns:wsa="http://www.w3.org/2005/08/addressing"
              xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <s12:Header>
        <wsa:To s12:mustUnderstand="true">https://127.0.0.1:51341/</wsa:To>
        <wsa:Action s12:mustUnderstand="true">
            http://standards.ieee.org/downloads/11073/11073-20701-2018/StateEventService/EpisodicAlertReport
        </wsa:Action>
        <wsa:MessageID>urn:uuid:095ea71f-d5cb-4a6f-a12b-19a84933efef</wsa:MessageID>
        <ns0:MyClIdentifier xmlns:ns0="http.local.com" wsa:IsReferenceParameter="true">
            urn:uuid:75793341-2aa2-4a33-adfe-c158a0ad2982
        </ns0:MyClIdentifier>
    </s12:Header>
    <s12:Body>
        <msg:EpisodicAlertReport MdibVersion="10"
                                 SequenceId="urn:uuid:2d6bfcf5-a29c-42e4-99e3-99ce4a4e0233"
                                 xmlns:msg="http://standards.ieee.org/downloads/11073/11073-10207-2017/message"
                                 xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                                 xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
            <msg:ReportPart>
                <msg:AlertState xsi:type="pm:AlertSystemState" DescriptorVersion="0" StateVersion="7"
                                ActivationState="On" LastSelfCheck="1688025072995" SelfCheckCount="8"
                                PresentPhysiologicalAlarmConditions="" PresentTechnicalAlarmConditions=""
                                DescriptorHandle="ASYS0"/>
            </msg:ReportPart>
        </msg:EpisodicAlertReport>
    </s12:Body>
</s12:Envelope>""")

        test_xml(b"""<?xml version='1.0' encoding='UTF-8'?>
<s12:Envelope xmlns:wse="http://schemas.xmlsoap.org/ws/2004/08/eventing"
              xmlns:wsa="http://www.w3.org/2005/08/addressing"
              xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
              xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <s12:Header>
        <wsa:To s12:mustUnderstand="true">https://127.0.0.1:51341/</wsa:To>
        <wsa:Action s12:mustUnderstand="true">
            http://standards.ieee.org/downloads/11073/11073-20701-2018/StateEventService/EpisodicAlertReport
        </wsa:Action>
        <wsa:MessageID>urn:uuid:095ea71f-d5cb-4a6f-a12b-19a84933efef</wsa:MessageID>
        <ns0:MyClIdentifier xmlns:ns0="http.local.com" wsa:IsReferenceParameter="true">
            urn:uuid:75793341-2aa2-4a33-adfe-c158a0ad2982
        </ns0:MyClIdentifier>
    </s12:Header>
    <s12:Body>
        <msg:EpisodicAlertReport MdibVersion="10"
                                 SequenceId="urn:uuid:2d6bfcf5-a29c-42e4-99e3-99ce4a4e0233"
                                 xmlns:msg="http://standards.ieee.org/downloads/11073/11073-10207-2017/message"
                                 xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
                                 xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension">
            <msg:ReportPart>
                <msg:AlertState xsi:type="pm:AlertSystemState" DescriptorVersion="0" StateVersion="7"
                                ActivationState="On" LastSelfCheck="1688025072995" SelfCheckCount="8"
                                PresentPhysiologicalAlarmConditions="" PresentTechnicalAlarmConditions=""
                                DescriptorHandle="ASYS0"/>
            </msg:ReportPart>
        </msg:EpisodicAlertReport>
        <msg:EpisodicMetricReport MdibVersion="1180245"
                                  SequenceId="urn:uuid:5cdce523-d532-4d36-b5c1-42bf2fa8e3b0" InstanceId="0"
                                  xmlns:msg="http://standards.ieee.org/downloads/11073/11073-10207-2017/message"
                                  xmlns:pm="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant">
            <msg:ReportPart>
                <msg:SourceMds>mds0</msg:SourceMds>
                <msg:MetricState xsi:type="pm:NumericMetricState" StateVersion="21894"
                    DescriptorHandle="numeric.ch1.vmd0" DescriptorVersion="0">
                    <pm:MetricValue Value="21894" DeterminationTime="1688126346037">
                        <pm:MetricQuality Validity="Vld" Mode="Demo" />
                    </pm:MetricValue>
                </msg:MetricState>
            </msg:ReportPart>
        </msg:EpisodicMetricReport>
    </s12:Body>
</s12:Envelope>""")


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestMdib)
        
        
if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
