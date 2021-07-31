import unittest
import time
import logging
import sdc11073
from sdc11073.sdcdevice import waveforms
from tests import mockstuff
from sdc11073 import pmtypes
from sdc11073.mdib import descriptorcontainers as dc
from sdc11073.definitions_sdc import SDC_v1_Definitions

CLIENT_VALIDATE = True

# data that is used in report
HANDLES = ("0x34F05506", "0x34F05501", "0x34F05500")
SAMPLES = {"0x34F05506": (5.566406, 5.712891, 5.712891, 5.712891, 5.800781),
           "0x34F05501": (0.1, -0.1, 1.0, 2.0, 3.0),
           "0x34F05500": (3.198242, 3.198242, 3.198242, 3.198242, 3.163574, 1.1)}


        
class TestDeviceWaveform(unittest.TestCase):
    
    def setUp(self):
        self.mdib = sdc11073.mdib.DeviceMdibContainer(SDC_v1_Definitions)
        self.domSchema = self.mdib.biceps_schema.participant_schema
        self.msgSchema = self.mdib.biceps_schema.message_schema

        # this structure is not realistic, but sufficient for what we need here.
        desc = dc.MdsDescriptorContainer(self.mdib.nsmapper,
                                         handle='42',
                                         parent_handle=None,
                                         )
        self.mdib.descriptions.add_object(desc)
        for h in HANDLES:
            desc = dc.RealTimeSampleArrayMetricDescriptorContainer(self.mdib.nsmapper,
                                                                   handle=h,
                                                                   parent_handle='42',
                                                                   )
            desc.SamplePeriod = 0.1
            desc.unit=pmtypes.CodedValue('abc')
            desc.MetricAvailability=pmtypes.MetricAvailability.CONTINUOUS
            desc.MetricCategory=pmtypes.MetricCategory.MEASUREMENT
            self.mdib.descriptions.add_object(desc)
        
        self.sdcDevice = None
        self.nsmapper = sdc11073.namespaces.DocNamespaceHelper()


    def tearDown(self):
        if self.sdcDevice:
            self.sdcDevice.stopAll()

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
            expectedCount = 1.0/period
            rt_sample_array = waveform_generators[h].get_next_sample_array()
            # sleep is not very precise, therefore verify that number of sample is in a certein range
            self.assertTrue(expectedCount-5 <= len(rt_sample_array.samples) <= expectedCount+5) #
            self.assertTrue(abs(now - rt_sample_array.determination_time) <= 0.02)
            self.assertEqual(rt_sample_array.activation_state, pmtypes.ComponentActivation.ON)
        ca = pmtypes.ComponentActivation # shortcut
        h = HANDLES[0]
        for actState in (ca.OFF, ca.FAILURE, ca.NOT_READY, ca.SHUTDOWN, ca.STANDBY):    
            self.mdib.setWaveformGeneratorActivationState(h, actState)    
            rt_sample_array = waveform_generators[h].get_next_sample_array()
            self.assertEqual(rt_sample_array.activation_state, actState)
            self.assertEqual(len(rt_sample_array.samples), 0)

        self.mdib.setWaveformGeneratorActivationState(h, pmtypes.ComponentActivation.ON)
        now = time.time()
        time.sleep(0.1)    
        rt_sample_array = waveform_generators[h].get_next_sample_array()
        self.assertEqual(rt_sample_array.activation_state, pmtypes.ComponentActivation.ON)
        self.assertTrue(len(rt_sample_array.samples) > 0)
        self.assertTrue(abs(now - rt_sample_array.determination_time) <= 0.02)

    def test_waveformSubscription(self):
        self._model = sdc11073.pysoap.soapenvelope.DPWSThisModel(manufacturer='Chinakracher GmbH',
                                                                 manufacturerUrl='www.chinakracher.com',
                                                                 modelName='BummHuba',
                                                                 modelNumber='1.0',
                                                                 modelUrl='www.chinakracher.com/bummhuba/model',
                                                                 presentationUrl='www.chinakracher.com/bummhuba/presentation')
        self._device = sdc11073.pysoap.soapenvelope.DPWSThisDevice(friendlyName='Big Bang Practice',
                                                                   firmwareVersion='0.99',
                                                                   serialNumber='87kabuuum889')
        
        tr = waveforms.TriangleGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.02)
        st = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveformperiod=2.0, sampleperiod=0.02)
        si = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveformperiod=5.0, sampleperiod=0.02)
        
        self.mdib.register_waveform_generator(HANDLES[0], tr)
        self.mdib.register_waveform_generator(HANDLES[1], st)
        self.mdib.register_waveform_generator(HANDLES[2], si)
        
        annotation = pmtypes.Annotation(pmtypes.CodedValue('a','b'))
        self.mdib.registerAnnotationGenerator(annotation,
                                              triggerHandle=HANDLES[2],
                                              annotatedHandles=(HANDLES[0], HANDLES[1], HANDLES[2]))
        
        self.wsDiscovery = mockstuff.MockWsDiscovery(['5.6.7.8'])
        uuid = None # let device create one
        self.sdcDevice = sdc11073.sdcdevice.SdcDevice(self.wsDiscovery, uuid, self._model, self._device, self.mdib, logLevel=logging.DEBUG)
        self.sdcDevice.startAll()
        testSubscr = mockstuff.TestDevSubscription(self.sdcDevice.mdib.sdc_definitions.Actions.Waveform, self.sdcDevice.mdib.biceps_schema)
        self.sdcDevice.subscriptionsManager._subscriptions. add_object(testSubscr)

        time.sleep(3)
        print (testSubscr.reports[-2].as_xml(pretty=True))
        print (testSubscr.reports[-1].as_xml(pretty=True))
        self.assertGreater(len(testSubscr.reports), 20)
        

def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestDeviceWaveform)


if __name__ == '__main__':
    _logger = logging.Logger('sdc.device.subscrMgr')
    _logger.setLevel(logging.DEBUG)

#    unittest.TextTestRunner(verbosity=2).run(suite())

    unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromName('test_device_waveform.TestDeviceWaveform.test_waveformSubscription'))
#    unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromName('test_device_waveform.TestDeviceWaveform.test_waveformGeneratorHandling'))
