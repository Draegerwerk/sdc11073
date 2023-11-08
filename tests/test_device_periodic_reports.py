import logging
import time
import unittest
from itertools import cycle

from sdc11073 import wsdiscovery
from sdc11073.consumer import SdcConsumer
from sdc11073.loghelper import basic_logging_setup
from sdc11073.observableproperties import ValuesCollector
from sdc11073.xml_types.pm_types import RetrievabilityMethod, RetrievabilityInfo, Retrievability, InstanceIdentifier
from tests import utils
from tests.mockstuff import SomeDevice

CLIENT_VALIDATE = True


# pylint: disable=protected-access

class Test_Device_PeriodicReports(unittest.TestCase):

    def setUp(self):
        basic_logging_setup()
        logging.getLogger('sdc.device.pReports').setLevel(logging.DEBUG)
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = wsdiscovery.WSDiscovery('127.0.0.1')
        self.wsd.start()

        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml')
        mdib = self.sdc_device.mdib
        # add RetrievabilityMethod.PERIODIC to descriptors, this will trigger the device to sent periodic reports
        periods = cycle([1.0, 2.0, 3.0])
        for descr in mdib.descriptions.objects:
            p = next(periods)
            retr_list = descr.get_retrievability()
            if len(retr_list) == 0:
                retr_list.append(Retrievability())
            retr_list[0].By.append(RetrievabilityInfo(RetrievabilityMethod.PERIODIC, update_period=p))
            descr.set_retrievability(retr_list)
        mdib.xtra.update_retrievability_lists()

        self.sdc_device.start_all()
        loc_validators = [InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.set_location(utils.random_location(), loc_validators)

        time.sleep(0.1)  # allow full init of device
        x_addr = self.sdc_device.get_xaddrs()
        self.sdc_client = SdcConsumer(x_addr[0],
                                      sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                      ssl_context_container=None,
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
        """Test waits 10 seconds and counts reports that have been received in that time."""
        self.sdc_client.start_all()

        metric_coll = ValuesCollector(self.sdc_client, 'periodic_metric_report', 5)
        alert_coll = ValuesCollector(self.sdc_client, 'periodic_alert_report', 2)
        comp_coll = ValuesCollector(self.sdc_client, 'periodic_component_report', 5)
        op_coll = ValuesCollector(self.sdc_client, 'periodic_operational_state_report', 5)
        context_coll = ValuesCollector(self.sdc_client, 'periodic_context_report', 2)

        # any of the result calls will raise a timeout error if expected number of samples
        # is not collected before timeout
        wait = 1
        time.sleep(10)

        reports = metric_coll.result(timeout=wait)
        self.assertEqual((len(reports)), 5, msg=f'metric_coll got {len(metric_coll._result)}')

        reports = alert_coll.result(timeout=wait)
        self.assertEqual((len(reports)), 2, msg=f'alert_coll got {len(alert_coll._result)}')

        reports = comp_coll.result(timeout=wait)
        self.assertEqual((len(reports)), 5, msg=f'comp_coll got {len(comp_coll._result)}')

        reports = op_coll.result(timeout=wait)
        self.assertEqual((len(reports)), 5, msg=f'op_coll got {len(op_coll._result)}')

        reports = context_coll.result(timeout=wait)
        self.assertEqual((len(reports)), 2, msg=f'context_coll got {len(context_coll._result)}')
