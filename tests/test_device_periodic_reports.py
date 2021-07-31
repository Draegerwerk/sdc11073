import unittest
import logging
import os
import time
from itertools import cycle
#pylint: disable=protected-access
from sdc11073.sdcclient import SdcClient
from sdc11073 import pmtypes, wsdiscovery
from sdc11073.location import SdcLocation
from sdc11073.msgtypes import RetrievabilityMethod, RetrievabilityInfo
from sdc11073.observableproperties import ValuesCollector
from tests.mockstuff import SomeDevice

CLIENT_VALIDATE=True

class Test_Device_PeriodicReports(unittest.TestCase):
    
    def setUp(self):
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = wsdiscovery.WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        location = SdcLocation(fac='tklx',
                               poc='CU1',
                               bed='Bed')
    
        self.sdc_device = SomeDevice.fromMdibFile(self.wsd, None, '70041_MDIB_Final.xml')
        # set retrievability by periodic reports in every other descriptor
        periods = cycle([1.0, 2.0, 3.0])
        change = True
        for descr in self.sdc_device.mdib.descriptions.objects:
            if change:
                p = next(periods)
                descr.retrievability.By.append(RetrievabilityInfo(RetrievabilityMethod.PERIODIC,
                                                                 update_period=p))
                # change = not change
        self.sdc_device.mdib.update_retrievability_lists()
        self.sdc_device.startAll()
        self._locValidators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.setLocation(location, self._locValidators)

        time.sleep(0.1) # allow full init of device
        logging.getLogger('sdc.device').setLevel(logging.DEBUG)
        logging.getLogger('sdc.device.subscrMgr').setLevel(logging.DEBUG)

        xAddr = self.sdc_device.getXAddrs()
        self.sdc_client = SdcClient(xAddr[0],
                          sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                          validate=CLIENT_VALIDATE)

        print ('############### setUp done {} ##############'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))


    def tearDown(self):
        print ('############### tearDown {}... ##############'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### tearDown {} ... ##############'.format(self._testMethodName))
        self.sdc_device.stopAll()
        self.sdc_client.stopAll()
        self.wsd.stop()


    def test_periodic_reports(self):
        # wait for the next PeriodicMetricReport
        self.sdc_client.startAll(subscribe_periodic_reports=True)
        metric_coll = ValuesCollector(self.sdc_client, 'periodicMetricReport', 5)
        alert_coll = ValuesCollector(self.sdc_client, 'periodicAlertReport', 5)
        comp_coll = ValuesCollector(self.sdc_client, 'periodicComponentReport', 5)
        op_coll = ValuesCollector(self.sdc_client, 'periodicOperationalStateReport', 5)
        context_coll = ValuesCollector(self.sdc_client, 'periodicContextReport', 2)
        # any of the result calls will raise an timeout error if expected number of samples
        # is not collected before timeout
        m_result = metric_coll.result(timeout=10)
        a_result = alert_coll.result(timeout=10)
        comp_result = comp_coll.result(timeout=10)
        op_result = op_coll.result(timeout=10)
        cont_result = context_coll.result(timeout=10)




def suite():
    return unittest.TestLoader().loadTestsFromTestCase(Test_Device_PeriodicReports)


if __name__ == '__main__':
    def mklogger(logFolder=None):
        import logging.handlers
        applog = logging.getLogger('sdc')
        if len(applog.handlers) == 0:
            ch = logging.StreamHandler()
            # create formatter
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
            # add formatter to ch
            ch.setFormatter(formatter)
            # add ch to logger
            applog.addHandler(ch)
            if logFolder is not None:
                ch2 = logging.handlers.RotatingFileHandler(os.path.join(logFolder, 'sdcclient.log'),
                                                           maxBytes=5000000,
                                                           backupCount=2)
                ch2.setLevel(logging.INFO)
                ch2.setFormatter(formatter)
                # add ch to logger
                applog.addHandler(ch2)
    
        applog.setLevel(logging.DEBUG)
    
    mklogger()
    unittest.TextTestRunner(verbosity=2).run(suite())
