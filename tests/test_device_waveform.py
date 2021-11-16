import time
import unittest

import sdc11073
from sdc11073 import pmtypes
from sdc11073.definitions_sdc import SDC_v1_Definitions
from sdc11073.mdib import descriptorcontainers as dc
from sdc11073.sdcdevice import waveforms
from sdc11073.dpws import ThisModel, ThisDevice
from tests import mockstuff

CLIENT_VALIDATE = True

# data that is used in report
HANDLES = ("0x34F05506", "0x34F05501", "0x34F05500")
SAMPLES = {"0x34F05506": (5.566406, 5.712891, 5.712891, 5.712891, 5.800781),
           "0x34F05501": (0.1, -0.1, 1.0, 2.0, 3.0),
           "0x34F05500": (3.198242, 3.198242, 3.198242, 3.198242, 3.163574, 1.1)}


class TestDeviceWaveform(unittest.TestCase):

    def setUp(self):
        self.mdib = sdc11073.mdib.DeviceMdibContainer(SDC_v1_Definitions)

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

        self.sdcDevice = None
        self.nsmapper = sdc11073.namespaces.DocNamespaceHelper()

    def tearDown(self):
        if self.sdcDevice:
            self.sdcDevice.stop_all()

    def test_waveformGeneratorHandling(self):
        tr = waveforms.TriangleGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.005)
        st = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.01)
        si = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveformperiod=5.0, sampleperiod=0.05)

        self.mdib.register_waveform_generator(HANDLES[0], tr)
        self.mdib.register_waveform_generator(HANDLES[1], st)
        self.mdib.register_waveform_generator(HANDLES[2], si)

        waveform_generators = self.mdib._waveform_source._waveform_generators
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
            expectedCount = 1.0 / period
            rt_sample_array = waveform_generators[h].get_next_sample_array()
            # sleep is not very precise, therefore verify that number of sample is in a certein range
            self.assertTrue(expectedCount - 5 <= len(rt_sample_array.samples) <= expectedCount + 5)  #
            self.assertTrue(abs(now - rt_sample_array.determination_time) <= 0.02)
            self.assertEqual(rt_sample_array.activation_state, pmtypes.ComponentActivation.ON)
        ca = pmtypes.ComponentActivation  # shortcut
        h = HANDLES[0]
        for actState in (ca.OFF, ca.FAILURE, ca.NOT_READY, ca.SHUTDOWN, ca.STANDBY):
            self.mdib.set_waveform_generator_activation_state(h, actState)
            rt_sample_array = waveform_generators[h].get_next_sample_array()
            self.assertEqual(rt_sample_array.activation_state, actState)
            self.assertEqual(len(rt_sample_array.samples), 0)

        self.mdib.set_waveform_generator_activation_state(h, pmtypes.ComponentActivation.ON)
        now = time.time()
        time.sleep(0.1)
        rt_sample_array = waveform_generators[h].get_next_sample_array()
        self.assertEqual(rt_sample_array.activation_state, pmtypes.ComponentActivation.ON)
        self.assertTrue(len(rt_sample_array.samples) > 0)
        self.assertTrue(abs(now - rt_sample_array.determination_time) <= 0.02)

    def test_waveformSubscription(self):
        # self._model = sdc11073.pysoap.soapenvelope.DPWSThisModel(manufacturer='Chinakracher GmbH',
        #                                                          manufacturer_url='www.chinakracher.com',
        #                                                          model_name='BummHuba',
        #                                                          model_number='1.0',
        #                                                          model_url='www.chinakracher.com/bummhuba/model',
        #                                                          presentation_url='www.chinakracher.com/bummhuba/presentation')
        # self._device = sdc11073.pysoap.soapenvelope.DPWSThisDevice(friendly_name='Big Bang Practice',
        #                                                            firmware_version='0.99',
        #                                                            serial_number='87kabuuum889')
        self._model = ThisModel(manufacturer='Chinakracher GmbH',
                                manufacturer_url='www.chinakracher.com',
                                model_name='BummHuba',
                                model_number='1.0',
                                model_url='www.chinakracher.com/bummhuba/model',
                                presentation_url='www.chinakracher.com/bummhuba/presentation')
        self._device = ThisDevice(friendly_name='Big Bang Practice',
                                  firmware_version='0.99',
                                  serial_number='87kabuuum889')

        tr = waveforms.TriangleGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.02)
        st = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.02)
        si = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveformperiod=5.0, sampleperiod=0.02)

        self.mdib.register_waveform_generator(HANDLES[0], tr)
        self.mdib.register_waveform_generator(HANDLES[1], st)
        self.mdib.register_waveform_generator(HANDLES[2], si)

        annotation = pmtypes.Annotation(pmtypes.CodedValue('a', 'b'))
        self.mdib.register_annotation_generator(annotation,
                                                trigger_handle=HANDLES[2],
                                                annotated_handles=(HANDLES[0], HANDLES[1], HANDLES[2]))

        self.wsDiscovery = mockstuff.MockWsDiscovery(['5.6.7.8'])
        self.sdcDevice = sdc11073.sdcdevice.SdcDevice(self.wsDiscovery, self._model, self._device, self.mdib)
        self.sdcDevice.start_all()
        testSubscr = mockstuff.TestDevSubscription([self.sdcDevice.mdib.sdc_definitions.Actions.Waveform],
                                                   self.sdcDevice.msg_factory)
        self.sdcDevice.subscriptions_manager._subscriptions.add_object(testSubscr)

        time.sleep(3)
        #print(testSubscr.reports[-2].as_xml(pretty=True))
        #print(testSubscr.reports[-1].as_xml(pretty=True))
        self.assertGreater(len(testSubscr.reports), 20)
