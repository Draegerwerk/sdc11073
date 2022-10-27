import logging
import time
import unittest

from sdc11073 import pmtypes, wsdiscovery
from sdc11073.location import SdcLocation
from tests.mockstuff import SomeDevice


# pylint: disable=protected-access

class Test_Device(unittest.TestCase):

    def setUp(self):
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = wsdiscovery.WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        location = SdcLocation(fac='fac1',
                               poc='CU1',
                               bed='Bed')
        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml')
        self.sdc_device.start_all()
        self._locValidators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.set_location(location, self._locValidators)

        time.sleep(0.1)  # allow full init of device

        print('############### setUp done {} ##############'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))

    def tearDown(self):
        print('############### tearDown {}... ##############'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### tearDown {} ... ##############'.format(self._testMethodName))
        self.sdc_device.stop_all()
        self.wsd.stop()

    def test_restart(self):
        """ Starting 2nd device with existing mdib shall not raise an exception"""
        self.sdc_device.stop_all()
        sdc_device2 = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml')
        try:
            sdc_device2.start_all()
        finally:
            sdc_device2.stop_all()
