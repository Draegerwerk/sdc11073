import unittest
import logging
import os
import time
#pylint: disable=protected-access
from sdc11073 import pmtypes, wsdiscovery
from sdc11073.location import SdcLocation
from tests.mockstuff import SomeDevice

class Test_Device(unittest.TestCase):
    
    def setUp(self):
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = wsdiscovery.WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        location = SdcLocation(fac='tklx',
                               poc='CU1',
                               bed='Bed')
    
#        self.sdcDevice = CoCoDeviceAnesthesia(self.wsd, my_uuid=None, useSSL=False)
        self.sdcDevice = SomeDevice.fromMdibFile(self.wsd, None, '70041_MDIB_Final.xml')
        self.sdcDevice.startAll()
        self._locValidators = [pmtypes.InstanceIdentifier('Validator', extensionString='System')]
        self.sdcDevice.setLocation(location, self._locValidators)

        time.sleep(0.1) # allow full init of device
        
        print ('############### setUp done {} ##############'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))


    def tearDown(self):
        print ('############### tearDown {}... ##############'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### tearDown {} ... ##############'.format(self._testMethodName))
        self.sdcDevice.stopAll()
        self.wsd.stop()


    def test_restart(self):
        ''' Starting 2nd device with existing mdib shall not raise an exception'''
        self.sdcDevice.stopAll()
#        sdcDevice2 = CoCoDeviceAnesthesia(self.wsd, my_uuid=None, useSSL=False,
#                                          deviceMdibContainer=self.sdcDevice.mdib)
        sdcDevice2 = SomeDevice.fromMdibFile(self.wsd, None, '70041_MDIB_Final.xml')
        try:
            sdcDevice2.startAll()
        finally:
            sdcDevice2.stopAll()
            


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(Test_Device)


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
