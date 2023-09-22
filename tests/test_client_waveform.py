import logging
import sys
import unittest

from lxml import etree as etree_

from sdc11073 import definitions_sdc, loghelper
from sdc11073.consumer import SdcConsumer
from sdc11073.mdib import ConsumerMdib
from sdc11073.mdib.descriptorcontainers import RealTimeSampleArrayMetricDescriptorContainer
from sdc11073.mdib.statecontainers import RealTimeSampleArrayMetricStateContainer
from sdc11073.namespaces import default_ns_helper as ns_hlp
from sdc11073.xml_types.pm_types import Coding

DEV_ADDRESS = 'http://127.0.0.1:10000'
CLIENT_VALIDATE = True

# data that is used in report
HANDLES = ("0x34F05506", "0x34F05501", "0x34F05500")
SAMPLES = {"0x34F05506": (5.566406, 5.712891, 5.712891, 5.712891, 5.800781),
           "0x34F05501": (0.1, -0.1, 1.0, 2.0, 3.0),
           "0x34F05500": (3.198242, 3.198242, 3.198242, 3.198242, 3.163574, 1.1)}

wf_report_template = """<?xml version="1.0" encoding="utf-8"?>
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
    <wsa5:Action SOAP-ENV:mustUnderstand="true">http://standards.ieee.org/downloads/11073/11073-20701-2018/WaveformService/WaveformStream</wsa5:Action>
    <wsa:Identifier xmlns:wsa="http://www.w3.org/2005/08/addressing">
    urn:uuid:9f00ba10-3ffe-47e9-8238-88339a4a457d</wsa:Identifier>
  </SOAP-ENV:Header>
  <SOAP-ENV:Body>
    <msg:WaveformStream MdibVersion="{mdib_version}" SequenceId="">
      <msg:State StateVersion="{state_version}"
      DescriptorHandle="0x34F05506" DescriptorVersion="2"
      xsi:type="dom:RealTimeSampleArrayMetricState">
        <dom:MetricValue xsi:type="dom:SampleArrayValue"
        Samples="{array1}"
        DeterminationTime="{obs_time}">
          <dom:MetricQuality Validity="Vld"></dom:MetricQuality>
        </dom:MetricValue>
      </msg:State>
      <msg:State StateVersion="{state_version}"
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
      <msg:State StateVersion="{state_version}"
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
"""


def _mk_wf_report(observation_time_ms: int, mdib_version: int, state_version: int) -> str:
    # helper to create a waveform report
    return wf_report_template.format(
        obs_time=observation_time_ms,
        array1=' '.join([str(n) for n in SAMPLES["0x34F05506"]]),
        array2=' '.join([str(n) for n in SAMPLES["0x34F05501"]]),
        array3=' '.join([str(n) for n in SAMPLES["0x34F05500"]]),
        msg='http://standards.ieee.org/downloads/11073/11073-10207-2017/message',
        ext='http://standards.ieee.org/downloads/11073/11073-10207-2017/extension',
        dom='http://standards.ieee.org/downloads/11073/11073-10207-2017/participant',
        mdib_version=mdib_version,
        state_version=state_version,
    )


class TestClientWaveform(unittest.TestCase):

    def setUp(self):
        loghelper.basic_logging_setup()
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)
        self.sdc_client = SdcConsumer(DEV_ADDRESS,
                                      sdc_definitions=definitions_sdc.SdcV1Definitions,
                                      ssl_context_container=None,
                                      validate=CLIENT_VALIDATE)

    def tearDown(self):
        sys.stderr.write(f'############### tearDown {self._testMethodName}... ##############\n')
        self.log_watcher.setPaused(True)
        self.sdc_client.stop_all()
        try:
            self.log_watcher.check()
        except loghelper.LogWatchError as ex:
            sys.stderr.write(repr(ex))
            raise
        sys.stderr.write(f'############### tearDown {self._testMethodName} done ##############\n')

    def test_basic_handling(self):
        """Call _onWaveformReport method directly. Verify that observable is a WaveformStream Element."""
        cl = self.sdc_client
        observation_time_ms = 1467596359152

        report = _mk_wf_report(observation_time_ms, 2, 42)
        data = cl.msg_reader.read_received_message(report.encode('utf-8'))
        cl._on_notification(data)

    def test_stream_handling(self):
        """Connect a mdib with client. Call _onWaveformReport method directly.

        Verify that observable is a WaveformStream Element.
        """
        observation_time_ms = 1467596359152  # something in a plausible range
        observation_time = observation_time_ms / 1000.0

        cl = self.sdc_client

        client_mdib = ConsumerMdib(cl)
        client_mdib._xtra.bind_to_client_observables()
        client_mdib._is_initialized = True  # fake it, because we do not call init_mdib()
        client_mdib.MDIB_VERSION_CHECK_DISABLED = True  # we have no mdib version incrementing in this test, therefore disable check
        # create dummy descriptors
        for handle in HANDLES:
            attributes = {'SamplePeriod': 'P0Y0M0DT0H0M0.0157S',  # use a unique sample period
                          etree_.QName(ns_hlp.ns_map['xsi'],
                                       'type'): 'dom:RealTimeSampleArrayMetricDescriptor',
                          'Handle': handle,
                          'DescriptorVersion': '2'}
            element = etree_.Element('Metric', attrib=attributes, nsmap=ns_hlp.ns_map)
            descr = RealTimeSampleArrayMetricDescriptorContainer.from_node(element, None)  # None = no parent handle
            client_mdib.descriptions.add_object(descr)
            state = RealTimeSampleArrayMetricStateContainer(descr)
            state.StateVersion = 41
            client_mdib.states.add_object(state)

        wf_report1 = _mk_wf_report(observation_time_ms, 2, 42)
        received_message_data = cl.msg_reader.read_received_message(wf_report1.encode('utf-8'))
        cl._on_notification(received_message_data)

        # verify that all handles of reported RealTimeSampleArrays are present
        for handle in HANDLES:
            current_samples = SAMPLES[handle]
            s_count = len(current_samples)
            rtBuffer = client_mdib.rt_buffers[handle]
            self.assertEqual(s_count, len(rtBuffer.rt_data))
            self.assertAlmostEqual(rtBuffer.sample_period, 0.0157)
            self.assertAlmostEqual(rtBuffer.rt_data[0].determination_time, observation_time)
            self.assertAlmostEqual(rtBuffer.rt_data[-1].determination_time - observation_time,
                                   rtBuffer.sample_period * (s_count - 1), places=4)
            self.assertAlmostEqual(rtBuffer.rt_data[-2].determination_time - observation_time,
                                   rtBuffer.sample_period * (s_count - 2), places=4)
            for i in range(s_count):
                self.assertAlmostEqual(float(rtBuffer.rt_data[i].value), current_samples[i])

        # verify that only handle 0x34F05501 has an annotation
        for handle in [HANDLES[0], HANDLES[2]]:
            rtBuffer = client_mdib.rt_buffers[handle]
            for sample in rtBuffer.rt_data:
                self.assertEqual(0, len(sample.annotations))

        rtBuffer = client_mdib.rt_buffers[HANDLES[1]]
        annotated = rtBuffer.rt_data[2]  # this object should have the annotation (SampleIndex="2")
        self.assertEqual(1, len(annotated.annotations))
        self.assertEqual(Coding('4711', 'bla'), annotated.annotations[0].Type.coding)
        for i in (0, 1, 3, 4):
            self.assertEqual(0, len(rtBuffer.rt_data[i].annotations))

        # add another Report (with identical data, but that is not relevant here)
        wf_report2 = _mk_wf_report(observation_time_ms + 100, 3, 43)
        received_message_data = cl.msg_reader.read_received_message(wf_report2.encode('utf-8'))
        cl._on_notification(received_message_data)
        # verify only that array length is 2*bigger now
        for handle in HANDLES:
            current_samples = SAMPLES[handle]
            s_count = len(current_samples)
            rtBuffer = client_mdib.rt_buffers[handle]
            self.assertEqual(s_count * 2, len(rtBuffer.rt_data))

        # add a lot more data, verify that length limitation is working
        for i in range(100):
            wf_report = _mk_wf_report(observation_time_ms + 100 * 1, 3 + 1, 43 + i)
            received_message_data = cl.msg_reader.read_received_message(wf_report.encode('utf-8'))
            cl._on_notification(received_message_data)
        # verify only that array length is limited
        for handle in HANDLES:
            rtBuffer = client_mdib.rt_buffers[handle]
            self.assertEqual(rtBuffer._max_samples, len(rtBuffer.rt_data))
