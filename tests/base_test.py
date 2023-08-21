import unittest
import os
from sdc11073.wsdiscovery import WSDiscoverySingleAdapter
from sdc11073 import pmtypes
from sdc11073.location import SdcLocation
from sdc11073.sdcclient import SdcClient
from tests import utils
from tests.mockstuff import SomeDevice

loopback_adapter = 'Loopback Pseudo-Interface 1' if os.name == 'nt' else 'lo'
"""
Base test to use in all test that require device or a client. This sets up a default device and client
and has connect method.
"""

class BaseTest(unittest.TestCase):

    def setUp(self):
        self.wsdiscovery = WSDiscoverySingleAdapter(loopback_adapter)
        self.wsdiscovery.start()
        self._locValidators = [pmtypes.InstanceIdentifier('Validator', extensionString='System')]

    def tearDown(self):
        self.wsdiscovery.stop()

    def setUpCocoDraft10(self):
        self.cocoFinalLocation = utils.random_location()

        self.sdcDeviceCoCoFinal = SomeDevice.fromMdibFile(self.wsdiscovery, None, '70041_MDIB_Final.xml')
        self.sdcDeviceCoCoFinal.startAll()
        self.sdcDeviceCoCoFinal.setLocation(self.cocoFinalLocation, self._locValidators)
        xAddr = self.sdcDeviceCoCoFinal.getXAddrs()
        self.sdcClientCocoFinal = SdcClient(xAddr[0],
                                            deviceType=self.sdcDeviceCoCoFinal.mdib.sdc_definitions.MedicalDeviceType,
                                            validate=True)
        self.sdcClientCocoFinal.startAll()

    def stopDraft10(self):
        self.sdcClientCocoFinal.stopAll()
        self.sdcDeviceCoCoFinal.stopAll()
