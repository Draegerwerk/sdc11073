import unittest
import logging
from lxml import etree as etree_
import sdc11073
from sdc11073 import namespaces
from sdc11073 import definitions_sdc

#pylint: disable=protected-access

DEV_ADDRESS = '169.254.0.200:10000'
CLIENT_VALIDATE = True

# data that is used in report
observationTime_ms = 1467596359152
OBSERVATIONTIME = observationTime_ms/1000.0
HANDLES = ("0x34F05506", "0x34F05501", "0x34F05500")
SAMPLES = {"0x34F05506": (5.566406, 5.712891, 5.712891, 5.712891, 5.800781),
           "0x34F05501": (0.1, -0.1, 1.0, 2.0, 3.0),
           "0x34F05500": (3.198242, 3.198242, 3.198242, 3.198242, 3.163574, 1.1)}

WfReport_draft6 = u'''<?xml version="1.0" encoding="utf-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope"
xmlns:SOAP-ENC="http://www.w3.org/2003/05/soap-encoding"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xmlns:xsd="http://www.w3.org/2001/XMLSchema"
xmlns:chan="http://schemas.microsoft.com/ws/2005/02/duplex"
xmlns:wsa5="http://www.w3.org/2005/08/addressing"
xmlns:ext="{ext}"
xmlns:dom="{dom}"
xmlns:dpws="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01"
xmlns:si="http://safety-information-uri/15/08"
xmlns:msg="{msg}"
xmlns:wsd11="http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01"
xmlns:wse4="http://schemas.xmlsoap.org/ws/2004/08/eventing"
xmlns:wst4="http://schemas.xmlsoap.org/ws/2004/09/transfer"
xmlns:wsx4="http://schemas.xmlsoap.org/ws/2004/09/mex">
  <SOAP-ENV:Header>
    <wsa5:MessageID>
    urn:uuid:904577a6-6012-4558-b772-59a9c90bacbb</wsa5:MessageID>
    <wsa5:To SOAP-ENV:mustUnderstand="true">
    http://169.254.0.99:62627</wsa5:To>
    <wsa5:Action SOAP-ENV:mustUnderstand="true">
    {msg}/15/04/Waveform/Waveform</wsa5:Action>
    <wsa:Identifier xmlns:wsa="http://www.w3.org/2005/08/addressing">
    urn:uuid:9f00ba10-3ffe-47e9-8238-88339a4a457d</wsa:Identifier>
  </SOAP-ENV:Header>
  <SOAP-ENV:Body>
    <msg:WaveformStreamReport MdibVersion="2" SequenceId="">
      <msg:State StateVersion="19716"
      DescriptorHandle="0x34F05506" DescriptorVersion="2"
      xsi:type="dom:RealTimeSampleArrayMetricState">
        <dom:MetricValue xsi:type="dom:SampleArrayValue"
        Samples="{array1}"
        DeterminationTime="{obs_time}">
          <dom:MetricQuality Validity="Vld"></dom:MetricQuality>
        </dom:MetricValue>
      </msg:State>
      <msg:State StateVersion="19715"
      DescriptorHandle="0x34F05501" DescriptorVersion="2"
      xsi:type="dom:RealTimeSampleArrayMetricState">
        <dom:MetricValue xsi:type="dom:SampleArrayValue"
        Samples="{array2}"
        DeterminationTime="{obs_time}">
          <dom:MetricQuality Validity="Vld"></dom:MetricQuality>
          <dom:Annotation><dom:Type Code="4711" CodingSystem="bla"/></dom:Annotation>
          <dom:ApplyAnnotation AnnotationIndex="0" SampleIndex="2"></dom:ApplyAnnotation>
        </dom:MetricValue>
      </msg:State>
      <msg:State StateVersion="19715"
      DescriptorHandle="0x34F05500" DescriptorVersion="2"
      xsi:type="dom:RealTimeSampleArrayMetricState">
        <dom:MetricValue xsi:type="dom:SampleArrayValue"
        Samples="{array3}"
        DeterminationTime="{obs_time}">
          <dom:MetricQuality Validity="Vld"></dom:MetricQuality>
        </dom:MetricValue>
      </msg:State>
    </msg:WaveformStreamReport>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
'''.format(obs_time=observationTime_ms, 
           array1=' '.join([str(n) for n in SAMPLES["0x34F05506"]]),
           array2=' '.join([str(n) for n in SAMPLES["0x34F05501"]]),
           array3=' '.join([str(n) for n in SAMPLES["0x34F05500"]]),
           msg=namespaces.nsmap['msg'], 
           ext=namespaces.nsmap['ext'], 
           dom=namespaces.nsmap['dom'],
          )


WfReport_draft10 = u'''<?xml version="1.0" encoding="utf-8"?>
<SOAP-ENV:Envelope xmlns:SOAP-ENV="http://www.w3.org/2003/05/soap-envelope"
xmlns:SOAP-ENC="http://www.w3.org/2003/05/soap-encoding"
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
xmlns:xsd="http://www.w3.org/2001/XMLSchema"
xmlns:chan="http://schemas.microsoft.com/ws/2005/02/duplex"
xmlns:wsa5="http://www.w3.org/2005/08/addressing"
xmlns:ext="{ext}"
xmlns:dom="{dom}"
xmlns:dpws="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01"
xmlns:si="http://safety-information-uri/15/08"
xmlns:msg="{msg}"
xmlns:wsd11="http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01"
xmlns:wse4="http://schemas.xmlsoap.org/ws/2004/08/eventing"
xmlns:wst4="http://schemas.xmlsoap.org/ws/2004/09/transfer"
xmlns:wsx4="http://schemas.xmlsoap.org/ws/2004/09/mex">
  <SOAP-ENV:Header>
    <wsa5:MessageID>
    urn:uuid:904577a6-6012-4558-b772-59a9c90bacbb</wsa5:MessageID>
    <wsa5:To SOAP-ENV:mustUnderstand="true">
    http://169.254.0.99:62627</wsa5:To>
    <wsa5:Action SOAP-ENV:mustUnderstand="true">
    {msg}/15/04/Waveform/Waveform</wsa5:Action>
    <wsa:Identifier xmlns:wsa="http://www.w3.org/2005/08/addressing">
    urn:uuid:9f00ba10-3ffe-47e9-8238-88339a4a457d</wsa:Identifier>
  </SOAP-ENV:Header>
  <SOAP-ENV:Body>
    <msg:WaveformStream MdibVersion="2" SequenceId="">
      <msg:State StateVersion="19716"
      DescriptorHandle="0x34F05506" DescriptorVersion="2"
      xsi:type="dom:RealTimeSampleArrayMetricState">
        <dom:MetricValue xsi:type="dom:SampleArrayValue"
        Samples="{array1}"
        DeterminationTime="{obs_time}">
          <dom:MetricQuality Validity="Vld"></dom:MetricQuality>
        </dom:MetricValue>
      </msg:State>
      <msg:State StateVersion="19715"
      DescriptorHandle="0x34F05501" DescriptorVersion="2"
      xsi:type="dom:RealTimeSampleArrayMetricState">
        <dom:MetricValue xsi:type="dom:SampleArrayValue"
        Samples="{array2}"
        DeterminationTime="{obs_time}">
          <dom:MetricQuality Validity="Vld"></dom:MetricQuality>
          <dom:Annotation><dom:Type Code="4711" CodingSystem="bla"/></dom:Annotation>
          <dom:ApplyAnnotation AnnotationIndex="0" SampleIndex="2"></dom:ApplyAnnotation>
        </dom:MetricValue>
      </msg:State>
      <msg:State StateVersion="19715"
      DescriptorHandle="0x34F05500" DescriptorVersion="2"
      xsi:type="dom:RealTimeSampleArrayMetricState">
        <dom:MetricValue xsi:type="dom:SampleArrayValue"
        Samples="{array3}"
        DeterminationTime="{obs_time}">
          <dom:MetricQuality Validity="Vld"></dom:MetricQuality>
        </dom:MetricValue>
      </msg:State>
    </msg:WaveformStream>
  </SOAP-ENV:Body>
</SOAP-ENV:Envelope>
'''.format(obs_time=observationTime_ms, 
           array1=' '.join([str(n) for n in SAMPLES["0x34F05506"]]),
           array2=' '.join([str(n) for n in SAMPLES["0x34F05501"]]),
           array3=' '.join([str(n) for n in SAMPLES["0x34F05500"]]),
           msg=namespaces.nsmap['msg'], 
           ext=namespaces.nsmap['ext'], 
           dom=namespaces.nsmap['dom'],
          )


class TestClientWaveform(unittest.TestCase):
    
    def setUp(self):
        self.sdcClient_final =  sdc11073.sdcclient.SdcClient(DEV_ADDRESS,
                                                             sdc_definitions=definitions_sdc.SDC_v1_Definitions,
                                                             #deviceType=definitions_sdc.SDC_v1_Definitions.MedicalDeviceType,
                                                             validate=CLIENT_VALIDATE,
                                                             my_ipaddress='169.254.0.3',
                                                             logLevel=logging.DEBUG)
        self.all_clients = (self.sdcClient_final,)


    def test_basic_handling(self):
        ''' call _onWaveformReport method directly. Verify that observable is a WaveformStream Element'''

        # same test for draft10 version
        cl = self.sdcClient_final
        soapenvelope = sdc11073.pysoap.soapenvelope.ReceivedSoap12Envelope.fromXMLString(WfReport_draft10.encode('utf-8'),
                                                                                          schema=cl._bicepsSchema.bmmSchema)
        cl._onWaveFormReport(soapenvelope)
        self.assertEqual(cl.waveFormReport.tag, namespaces.msgTag('WaveformStream'))


    def test_stream_handling(self):
        ''' Connect a mdib with client. Call _onWaveformReport method directly. Verify that observable is a WaveformStream Element'''
        my_handles = ('0x34F05506', '0x34F05501', '0x34F05500')
        for cl, wfReport in ((self.sdcClient_final, WfReport_draft10),):
            clientmdib = sdc11073.mdib.ClientMdibContainer(cl)
            clientmdib._bindToObservables()
            clientmdib._isInitialized = True # fake it, because we do not call initMdib()
            clientmdib.MDIB_VERSION_CHECK_DISABLED = True # we have no mdib version incrementing in this test, therefore disable check
            
            # create dummy descriptors
            for handle in my_handles:
                attributes = {'SamplePeriod': 'P0Y0M0DT0H0M0.0157S',  # use a unique sample period
                              etree_.QName(sdc11073.namespaces.nsmap['xsi'], 'type'): 'dom:RealTimeSampleArrayMetricDescriptor',
                              'Handle':handle}
                element = etree_.Element('Metric', attrib=attributes, nsmap=sdc11073.namespaces.nsmap)
                clientmdib.descriptions.addObject(sdc11073.mdib.descriptorcontainers.RealTimeSampleArrayMetricDescriptorContainer.from_node(clientmdib.nsmapper, element, None)) # None = no parent handle
            soapenvelope = sdc11073.pysoap.soapenvelope.ReceivedSoap12Envelope.fromXMLString(wfReport.encode('utf-8'))
            cl._onWaveFormReport(soapenvelope)
            
            # verify that all handles of reported RealTimeSampleArrays are present
            for handle in my_handles:
                current_samples = SAMPLES[handle]
                s_count = len(current_samples)
                rtBuffer = clientmdib.rtBuffers[handle]
                self.assertEqual(len(rtBuffer.rt_data), s_count)
                self.assertAlmostEqual(rtBuffer.sample_period, 0.0157)
                self.assertAlmostEqual(rtBuffer.rt_data[0].observationTime, OBSERVATIONTIME)
                self.assertAlmostEqual(rtBuffer.rt_data[-1].observationTime - OBSERVATIONTIME, rtBuffer.sample_period*(s_count-1), places=4)
                self.assertAlmostEqual(rtBuffer.rt_data[-2].observationTime - OBSERVATIONTIME, rtBuffer.sample_period*(s_count-2), places=4)
                for i in range(s_count):
                    self.assertAlmostEqual(rtBuffer.rt_data[i].value, current_samples[i])
            
            # verify that only handle 0x34F05501 has an annotation
            for handle in [my_handles[0], my_handles[2]]:
                rtBuffer = clientmdib.rtBuffers[handle]
                for sample in rtBuffer.rt_data:
                    self.assertEqual(len(sample.annotations), 0)
    
            rtBuffer = clientmdib.rtBuffers[my_handles[1]]
            annotated = rtBuffer.rt_data[2] # this object should have the annotation (SampleIndex="2")
            self.assertEqual(len(annotated.annotations), 1)
            self.assertEqual(annotated.annotations[0].coding.code, '4711')
            self.assertEqual(annotated.annotations[0].coding.codingSystem, 'bla')
            for i in (0,1,3,4):
                self.assertEqual(len(rtBuffer.rt_data[i].annotations), 0)
    
            # add another Report (with identical data, but that is not relevant here)
            soapenvelope = sdc11073.pysoap.soapenvelope.ReceivedSoap12Envelope.fromXMLString(wfReport.encode('utf-8'))
            cl._onWaveFormReport(soapenvelope)
            # verify only that array length is 2*bigger now
            for handle in my_handles:
                current_samples = SAMPLES[handle]
                s_count = len(current_samples)
                rtBuffer = clientmdib.rtBuffers[handle]
                self.assertEqual(len(rtBuffer.rt_data), s_count*2)
            
            #add a lot more data, verify that length limitation is working
            for i in range(100):
                soapenvelope = sdc11073.pysoap.soapenvelope.ReceivedSoap12Envelope.fromXMLString(wfReport.encode('utf-8'))
                cl._onWaveFormReport(soapenvelope)
            # verify only that array length is limited
            for handle in my_handles:
                current_samples = SAMPLES[handle]
                s_count = len(current_samples)
                rtBuffer = clientmdib.rtBuffers[handle]
                self.assertEqual(len(rtBuffer.rt_data), rtBuffer._max_samples)



def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestClientWaveform)


if __name__ == '__main__':
    logging.getLogger('sdc.client').setLevel(logging.DEBUG)
    
    unittest.TextTestRunner(verbosity=2).run(suite())
#   unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromName('test_client_waveform.TestClientWafeform.test_stream_handling'))
    
        