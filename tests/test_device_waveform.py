import time
import unittest

import sdc11073
from sdc11073 import pmtypes
from sdc11073.dpws import ThisModelType, ThisDeviceType
from sdc11073.mdib import descriptorcontainers as dc
from sdc11073.mdib.devicewaveform import Annotator
from sdc11073.sdcdevice import waveforms
from tests import mockstuff

# data that is used in report
HANDLES = ("0x34F05506", "0x34F05501", "0x34F05500")


class TestDeviceWaveform(unittest.TestCase):

    def setUp(self):
        self.mdib = sdc11073.mdib.DeviceMdibContainer()

        # this structure is not realistic, but sufficient for what we need here.
        desc = dc.MdsDescriptorContainer(handle='42', parent_handle=None)
        self.mdib.descriptions.add_object(desc)
        for h in HANDLES:
            desc = dc.RealTimeSampleArrayMetricDescriptorContainer(handle=h, parent_handle='42')
            desc.SamplePeriod = 0.1
            desc.unit = pmtypes.CodedValue('abc')
            desc.MetricAvailability = pmtypes.MetricAvailability.CONTINUOUS
            desc.MetricCategory = pmtypes.MetricCategory.MEASUREMENT
            self.mdib.descriptions.add_object(desc)

        self.sdc_device = None

    def tearDown(self):
        if self.sdc_device:
            self.sdc_device.stop_all()

    def test_waveformGeneratorHandling(self):
        waveform_provider = self.mdib.xtra.waveform_provider
        self.assertIsNotNone(waveform_provider)

        tr = waveforms.TriangleGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.005)
        st = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.01)
        si = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveformperiod=5.0, sampleperiod=0.05)

        waveform_provider.register_waveform_generator(HANDLES[0], tr)
        waveform_provider.register_waveform_generator(HANDLES[1], st)
        waveform_provider.register_waveform_generator(HANDLES[2], si)

        waveform_generators = waveform_provider._waveform_generators
        # first read shall always be empty
        for h in HANDLES:
            rt_sample_array = waveform_generators[h].get_next_sample_array()
            self.assertEqual(rt_sample_array.activation_state, pmtypes.ComponentActivation.ON)
            self.assertEqual(len(rt_sample_array.samples), 0)
        # collect some samples
        now = time.time()
        time.sleep(1)
        for h in HANDLES:
            period = waveform_generators[h]._generator.sampleperiod
            expected_count = 1.0 / period
            rt_sample_array = waveform_generators[h].get_next_sample_array()
            # sleep is not very precise, therefore verify that number of sample is in a certain range
            self.assertTrue(expected_count - 5 <= len(rt_sample_array.samples) <= expected_count + 5)  #
            self.assertTrue(abs(now - rt_sample_array.determination_time) <= 0.02)
            self.assertEqual(rt_sample_array.activation_state, pmtypes.ComponentActivation.ON)
        h = HANDLES[0]
        # test with all activation states
        for act_state in pmtypes.ComponentActivation:
            waveform_provider.set_activation_state(h, act_state)
            rt_sample_array = waveform_generators[h].get_next_sample_array()
            self.assertEqual(rt_sample_array.activation_state, act_state)
            self.assertEqual(len(rt_sample_array.samples), 0)

        waveform_provider.set_activation_state(h, pmtypes.ComponentActivation.ON)
        now = time.time()
        time.sleep(0.1)
        rt_sample_array = waveform_generators[h].get_next_sample_array()
        self.assertEqual(rt_sample_array.activation_state, pmtypes.ComponentActivation.ON)
        self.assertTrue(len(rt_sample_array.samples) > 0)
        self.assertTrue(abs(now - rt_sample_array.determination_time) <= 0.02)

    def test_waveformSubscription(self):
        this_model = ThisModelType(manufacturer='ABCDEFG GmbH',
                                   manufacturer_url='www.abcdefg.com',
                                   model_name='Foobar',
                                   model_number='1.0',
                                   model_url='www.abcdefg.com/foobar/model',
                                   presentation_url='www.abcdefg.com/foobar/presentation')
        this_device = ThisDeviceType(friendly_name='Big Bang Practice',
                                     firmware_version='0.99',
                                     serial_number='123serial')

        waveform_provider = self.mdib.xtra.waveform_provider
        self.assertIsNotNone(waveform_provider)

        tr = waveforms.TriangleGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.02)
        st = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.02)
        si = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveformperiod=5.0, sampleperiod=0.02)

        waveform_provider.register_waveform_generator(HANDLES[0], tr)
        waveform_provider.register_waveform_generator(HANDLES[1], st)
        waveform_provider.register_waveform_generator(HANDLES[2], si)

        annotator = Annotator(annotation=pmtypes.Annotation(pmtypes.CodedValue('a', 'b')),
                              trigger_handle=HANDLES[2],
                              annotated_handles=[HANDLES[0], HANDLES[1], HANDLES[2]])
        waveform_provider.register_annotation_generator(annotator)

        wsd = mockstuff.MockWsDiscovery(['5.6.7.8'])
        self.sdc_device = sdc11073.sdcdevice.SdcDevice(wsd, this_model, this_device, self.mdib)
        self.sdc_device.start_all()
        test_subscription = mockstuff.TestDevSubscription([self.sdc_device.mdib.sdc_definitions.Actions.Waveform],
                                                          self.sdc_device.msg_factory)
        mgr = self.sdc_device.hosted_services.state_event_service.hosting_service.subscriptions_manager
        mgr._subscriptions.add_object(test_subscription)

        time.sleep(3)
        self.assertGreater(len(test_subscription.reports), 20)
