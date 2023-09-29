import time
import unittest
from decimal import Decimal
import sdc11073
from sdc11073.xml_types import pm_types
from sdc11073.xml_types.dpws_types import ThisModelType, ThisDeviceType
from sdc11073.mdib import descriptorcontainers as dc
from sdc11073.roles.waveformprovider import waveforms
from sdc11073.pysoap.soapclientpool import SoapClientPool
from sdc11073.roles.waveformprovider.waveformproviderimpl import GenericWaveformProvider
from tests import mockstuff

# data that is used in report
HANDLES = ("0x34F05506", "0x34F05501", "0x34F05500")


class TestDeviceWaveform(unittest.TestCase):

    def setUp(self):
        self.mdib = sdc11073.mdib.ProviderMdib()
        self._soap_client_pool = SoapClientPool(soap_client_factory=None, log_prefix="")

        # this structure is not realistic, but sufficient for what we need here.
        desc = dc.MdsDescriptorContainer(handle='some_new_handle', parent_handle=None)
        self.mdib.descriptions.add_object(desc)
        for h in HANDLES:
            desc = dc.RealTimeSampleArrayMetricDescriptorContainer(handle=h, parent_handle='some_new_handle')
            desc.SamplePeriod = 0.01
            desc.Unit = pm_types.CodedValue('abc')
            desc.TechnicalRange.append(pm_types.Range(Decimal(0), Decimal(10)))
            desc.MetricAvailability = pm_types.MetricAvailability.CONTINUOUS
            desc.MetricCategory = pm_types.MetricCategory.MEASUREMENT
            desc.Resolution = Decimal(0.01)
            self.mdib.descriptions.add_object(desc)
        self.mdib.xtra.mk_state_containers_for_all_descriptors()

        self.sdc_device = None

    def tearDown(self):
        if self.sdc_device:
            self.sdc_device.stop_all()

    def test_waveformGeneratorHandling(self):
        waveform_provider = GenericWaveformProvider(self.mdib, '')
        waveform_provider.provide_waveforms()

        waveform_generators = waveform_provider._waveform_generators
        # first read shall always be empty
        for h in HANDLES:
            rt_sample_array = waveform_generators[h].get_next_sample_array()
            self.assertEqual(rt_sample_array.activation_state, pm_types.ComponentActivation.ON)
            self.assertEqual(len(rt_sample_array.samples), 0)
        # collect some samples
        now = time.time()
        time.sleep(1)
        for h in HANDLES:
            period = waveform_generators[h]._generator.sample_period
            expected_count = 1.0 / period
            rt_sample_array = waveform_generators[h].get_next_sample_array()
            # sleep is not very precise, therefore verify that number of sample is in a certain range
            self.assertTrue(expected_count - 50 <= len(rt_sample_array.samples) <= expected_count + 50)  #
            self.assertTrue(abs(now - rt_sample_array.determination_time) <= 0.1)
            self.assertEqual(rt_sample_array.activation_state, pm_types.ComponentActivation.ON)
        h = HANDLES[0]
        # test with all activation states
        for act_state in pm_types.ComponentActivation:
            waveform_provider.set_activation_state(h, act_state)
            rt_sample_array = waveform_generators[h].get_next_sample_array()
            self.assertEqual(rt_sample_array.activation_state, act_state)
            self.assertEqual(len(rt_sample_array.samples), 0)

        waveform_provider.set_activation_state(h, pm_types.ComponentActivation.ON)
        now = time.time()
        time.sleep(0.1)
        rt_sample_array = waveform_generators[h].get_next_sample_array()
        self.assertEqual(rt_sample_array.activation_state, pm_types.ComponentActivation.ON)
        self.assertTrue(len(rt_sample_array.samples) > 0)
        self.assertTrue(abs(now - rt_sample_array.determination_time) <= 0.1)

    def test_waveformSubscription(self):
        self._mk_device()

        test_subscription = mockstuff.TestDevSubscription([self.sdc_device.mdib.sdc_definitions.Actions.Waveform],
                                                          self._soap_client_pool,
                                                          self.sdc_device.msg_factory)
        mgr = self.sdc_device.hosted_services.state_event_service.hosting_service.subscriptions_manager
        mgr._subscriptions.add_object(test_subscription)

        time.sleep(3)
        self.assertGreater(len(test_subscription.reports), 10)

    def _mk_device(self):
        this_model = ThisModelType(manufacturer='ABCDEFG GmbH',
                                   manufacturer_url='www.abcdefg.com',
                                   model_name='Foobar',
                                   model_number='1.0',
                                   model_url='www.abcdefg.com/foobar/model',
                                   presentation_url='www.abcdefg.com/foobar/presentation')
        this_device = ThisDeviceType(friendly_name='Big Bang Practice',
                                     firmware_version='0.99',
                                     serial_number='123serial')

        wsd = mockstuff.MockWsDiscovery('5.6.7.8')
        self.sdc_device = sdc11073.provider.SdcProvider(wsd, this_model, this_device, self.mdib)
        self.sdc_device.start_all()

        waveform_provider = self.sdc_device.waveform_provider

        tr = waveforms.TriangleGenerator(min_value=0, max_value=10, waveform_period=2.0, sample_period=0.02)
        st = waveforms.SawtoothGenerator(min_value=0, max_value=10, waveform_period=2.0, sample_period=0.02)
        si = waveforms.SinusGenerator(min_value=-8.0, max_value=10.0, waveform_period=5.0, sample_period=0.02)

        waveform_provider.register_waveform_generator(HANDLES[0], tr)
        waveform_provider.register_waveform_generator(HANDLES[1], st)
        waveform_provider.register_waveform_generator(HANDLES[2], si)

        waveform_provider.add_annotation_generator(pm_types.CodedValue('a', 'b'),
                                                   trigger_handle=HANDLES[2],
                                                   annotated_handles=[HANDLES[0], HANDLES[1], HANDLES[2]]
                                                   )

