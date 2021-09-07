import logging
import time
import unittest
from itertools import cycle

from sdc11073 import pmtypes, wsdiscovery
from sdc11073.location import SdcLocation
from sdc11073.msgtypes import RetrievabilityMethod, RetrievabilityInfo
from sdc11073.observableproperties import ValuesCollector
# pylint: disable=protected-access
from sdc11073.sdcclient import SdcClient
from tests.mockstuff import SomeDevice
from sdc11073.loghelper import basic_logging_setup

CLIENT_VALIDATE = True


class Test_Device_PeriodicReports(unittest.TestCase):

    def setUp(self):
        basic_logging_setup()
        #logging.getLogger('sdc.device.GenericAlarmProvider').setLevel(logging.DEBUG)
        logging.getLogger('sdc.device.pReports').setLevel(logging.DEBUG)
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = wsdiscovery.WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        location = SdcLocation(fac='tklx',
                               poc='CU1',
                               bed='Bed')

        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml')
        mdib = self.sdc_device.mdib
        # set retrievability by periodic reports in every other descriptor
        periods = cycle([1.0, 2.0, 3.0])
        change = True
        for descr in mdib.descriptions.objects:
            if change:
                p = next(periods)
                print (descr, p)
                try:
                    descr.retrievability.By.append(RetrievabilityInfo(RetrievabilityMethod.PERIODIC,
                                                                      update_period=p))
                    print(f'set {descr.Handle} to {p}')
                except:
                    print('!!!', descr)
                # change = not change
        mdib.update_retrievability_lists()
        self.sdc_device.start_all()
        self._locValidators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.set_location(location, self._locValidators)

        time.sleep(0.1)  # allow full init of device
        xAddr = self.sdc_device.get_xaddrs()
        self.sdc_client = SdcClient(xAddr[0],
                                    sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                    ssl_context=None,
                                    validate=CLIENT_VALIDATE)

        print('############### setUp done {} ##############'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))

    def tearDown(self):
        print('############### tearDown {}... ##############'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### tearDown {} ... ##############'.format(self._testMethodName))
        self.sdc_device.stop_all()
        self.sdc_client.stop_all()
        self.wsd.stop()

    def test_periodic_reports(self):
        # wait for the next PeriodicMetricReport
        self.sdc_client.start_all(subscribe_periodic_reports=True)
        metric_coll = ValuesCollector(self.sdc_client, 'periodic_metric_report', 5)
        alert_coll = ValuesCollector(self.sdc_client, 'periodic_alert_report', 2)
        comp_coll = ValuesCollector(self.sdc_client, 'periodic_component_report', 5)
        op_coll = ValuesCollector(self.sdc_client, 'periodic_operational_state_report', 5)
        context_coll = ValuesCollector(self.sdc_client, 'periodic_context_report', 2)
        # any of the result calls will raise an timeout error if expected number of samples
        # is not collected before timeout
        wait = 1
        time.sleep(10)
        print(f'metric_coll={len(metric_coll._result)}')
        m_result = metric_coll.result(timeout=wait)

        print(f'alert_coll={len(alert_coll._result)}')
        a_result = alert_coll.result(timeout=wait)

        print(f'comp_coll={len(comp_coll._result)}')
        comp_result = comp_coll.result(timeout=wait)

        print(f'op_coll={len(op_coll._result)}')
        op_result = op_coll.result(timeout=wait)

        print(f'context_coll={len(context_coll._result)}')
        cont_result = context_coll.result(timeout=wait)
