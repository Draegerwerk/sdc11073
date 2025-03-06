"""Unit tests for XML utility functions in the `xml_utils` module."""

import copy
import unittest

from lxml import etree

from sdc11073 import xml_utils
from tests import utils


class TestXmlParsing(unittest.TestCase):
    xml_to_be_parsed = (b"""<?xml version='1.0' encoding='UTF-8'?>
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
</s12:Envelope>""",
                        b"""<?xml version='1.0' encoding='UTF-8'?>
<s12:Envelope xmlns:wse="http://schemas.xmlsoap.org/ws/2004/08/eventing"
              xmlns:wsa="http://www.w3.org/2005/08/addressing"
              xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
              xmlns:msg="http://standards.ieee.org/downloads/11073/11073-10207-2017/message"
              xmlns="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant"
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
                <msg:AlertState xsi:type="AlertSystemState" DescriptorVersion="0" StateVersion="7"
                                ActivationState="On" LastSelfCheck="1688025072995" SelfCheckCount="8"
                                PresentPhysiologicalAlarmConditions="" PresentTechnicalAlarmConditions=""
                                DescriptorHandle="ASYS0"/>
            </msg:ReportPart>
        </msg:EpisodicAlertReport>
        <msg:EpisodicAlertReport MdibVersion="10"
                                 SequenceId="urn:uuid:2d6bfcf5-a29c-42e4-99e3-99ce4a4e0234">
            <msg:ReportPart>
                <msg:AlertState xsi:type="pm:AlertSystemState" DescriptorVersion="0" StateVersion="7"
                                ActivationState="On" LastSelfCheck="1688025072995" SelfCheckCount="8"
                                PresentPhysiologicalAlarmConditions="" PresentTechnicalAlarmConditions=""
                                DescriptorHandle="ASYS1"/>
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
</s12:Envelope>""",
                        b"""<?xml version='1.0' encoding='UTF-8'?>
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
</s12:Envelope>""",
                        b"""<?xml version='1.0' encoding='UTF-8'?>
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
</s12:Envelope>""",
                        b"""<?xml version="1.0" encoding="utf-8"?>
<ns0:Envelope xmlns:ns0="http://www.w3.org/2003/05/soap-envelope">
    <ns0:Header>
        <ns1:To xmlns:ns1="http://www.w3.org/2005/08/addressing">https://127.0.0.1:43165/</ns1:To>
        <ns2:MessageID xmlns:ns2="http://www.w3.org/2005/08/addressing">
            urn:uuid:urn:uuid:0561269f-45ad-4e69-9e3c-9954b7875605</ns2:MessageID>
        <ns3:Action xmlns:ns3="http://www.w3.org/2005/08/addressing">
            http://standards.ieee.org/downloads/11073/11073-20701-2018/WaveformService/WaveformStream</ns3:Action>
        <ns4:MyClIdentifier xmlns:ns4="http.local.com" ns5:IsReferenceParameter="true"
            xmlns:ns5="http://www.w3.org/2005/08/addressing">
            urn:uuid:be60a76f-ad2d-4cb4-88e2-4e983baea5ac</ns4:MyClIdentifier>
    </ns0:Header>
    <ns0:Body>
        <ns0:WaveformStream xmlns:ns0="__BICEPS_MessageModel__" MdibVersion="201"
            SequenceId="1f2fb5dc-c59f-4c9d-bb92-50855c3a54f0" InstanceId="0">
            <ns0:State StateVersion="141" DescriptorHandle="rtsa_metric_2.channel_1.vmd_0.mds_0"
                DescriptorVersion="0">
                <ns1:MetricValue xmlns:ns1="__BICEPS_ParticipantModel__"
                    DeterminationTime="1691316045908"
                    Samples="25">
                    <ns1:MetricQuality Validity="Vld" Mode="Demo"></ns1:MetricQuality>
                </ns1:MetricValue>
            </ns0:State>
            <ns0:State StateVersion="141" DescriptorHandle="rtsa_metric_1.channel_1.vmd_0.mds_0"
                DescriptorVersion="0">
                <ns2:MetricValue xmlns:ns2="__BICEPS_ParticipantModel__"
                    DeterminationTime="1691316045908"
                    Samples="25">
                    <ns2:MetricQuality Validity="Vld" Mode="Demo"></ns2:MetricQuality>
                </ns2:MetricValue>
            </ns0:State>
            <ns0:State StateVersion="141" DescriptorHandle="rtsa_metric_0.channel_1.vmd_0.mds_0"
                DescriptorVersion="0">
                <ns3:MetricValue xmlns:ns3="__BICEPS_ParticipantModel__"
                    DeterminationTime="1691316045908"
                    Samples="25">
                    <ns3:MetricQuality Validity="Vld" Mode="Demo"></ns3:MetricQuality>
                </ns3:MetricValue>
            </ns0:State>
        </ns0:WaveformStream>
    </ns0:Body>
</ns0:Envelope>""",
                        b"""<?xml version='1.0' encoding='UTF-8'?>
<s12:Envelope xmlns:wse="http://schemas.xmlsoap.org/ws/2004/08/eventing"
    xmlns:wsa="http://www.w3.org/2005/08/addressing"
    xmlns:s12="http://www.w3.org/2003/05/soap-envelope"
    xmlns="http://standards.ieee.org/downloads/11073/11073-10207-2017/message"
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
        <EpisodicAlertReport MdibVersion="10"
            SequenceId="urn:uuid:2d6bfcf5-a29c-42e4-99e3-99ce4a4e0233">
            <ReportPart>
                <AlertState xsi:type="pm:AlertSystemState" DescriptorVersion="0" StateVersion="7"
                    ActivationState="On" LastSelfCheck="1688025072995" SelfCheckCount="8"
                    PresentPhysiologicalAlarmConditions="" PresentTechnicalAlarmConditions=""
                    DescriptorHandle="ASYS0" />
            </ReportPart>
            <ReportPart>
                <AlertState xsi:type="pm:AlertSystemStateh" DescriptorVersion="0" StateVersion="7"
                    ActivationState="On" LastSelfCheck="168802507295" SelfCheckCount="8"
                    PresentPhysiologicalAlarmConditions="" PresentTechnicalAlarmConditions=""
                    DescriptorHandle="ASYS7" />
            </ReportPart>
        </EpisodicAlertReport>
    </s12:Body>
</s12:Envelope>""")

    def setUp(self) -> None:
        self.xml_to_be_tested = [etree.fromstring(raw_xml)[1] for raw_xml in self.xml_to_be_parsed]

    def _compare_nodes(self, expected: xml_utils.LxmlElement, actual: xml_utils.LxmlElement):
        self.assertNotEqual(id(expected), id(actual))
        self.assertEqual(expected.tag, actual.tag)
        self.assertEqual(expected.text, actual.text)
        self.assertEqual(expected.tail, actual.tail)
        self.assertDictEqual(dict(expected.attrib), dict(actual.attrib))  # make order of attributes irrelevant
        self.assertDictEqual(expected.nsmap, actual.nsmap)
        self.assertEqual(len(expected), len(actual))
        for c1, c2 in zip(expected, actual):
            self._compare_nodes(c1, c2)

    def test_copy_full_node(self):
        for soap_body in self.xml_to_be_tested:
            for report in soap_body:
                new_report = xml_utils.copy_element(report)
                self._compare_nodes(report.getroottree().getroot(), new_report.getroottree().getroot())

    def test_copy_node_wo_parent(self):
        for soap_body in self.xml_to_be_tested:
            for report in soap_body:
                new_report = xml_utils.copy_node_wo_parent(report)
                self.assertEqual(new_report.getparent(), None)
                self._compare_nodes(report, new_report)

    def test_lxml_element_type(self):
        self.assertEqual(etree._Element, xml_utils.LxmlElement)
        parsed_xml = etree.fromstring(self.xml_to_be_parsed[0])
        self.assertEqual(type(parsed_xml), xml_utils.LxmlElement)
        self.assertTrue(isinstance(parsed_xml, etree._Element))
        self.assertTrue(isinstance(parsed_xml, xml_utils.LxmlElement))

    def test_custom_qname(self):
        qname = utils.random_qname()
        new_qname = xml_utils.QName(qname.text)
        self.assertEqual(qname.text, new_qname.text)
        self.assertEqual(qname.localname, new_qname.localname)
        self.assertEqual(qname.namespace, new_qname.namespace)

    def test_custom_qname_copy(self):
        qname = xml_utils.QName(utils.random_qname().text)
        new_qname = copy.copy(qname)
        self.assertEqual(qname.text, new_qname.text)
        self.assertEqual(qname.localname, new_qname.localname)
        self.assertEqual(qname.namespace, new_qname.namespace)

    def test_custom_qname_deepcopy(self):
        qname = xml_utils.QName(utils.random_qname().text)
        new_qname = copy.deepcopy(qname)
        self.assertEqual(qname.text, new_qname.text)
        self.assertEqual(qname.localname, new_qname.localname)
        self.assertEqual(qname.namespace, new_qname.namespace)
