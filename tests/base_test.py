import unittest
from sdc11073.wsdiscovery import WSDiscoveryWhitelist, WSDiscoverySingleAdapter
from sdc11073 import pmtypes
from sdc11073.location import SdcLocation
from sdc11073.sdcclient import SdcClient
from tests.mockstuff import SomeDevice

"""
Base test to use in all test that require device or a client. This sets up a default device and client
and has connect method.
"""

class BaseTest(unittest.TestCase):

    def setUp(self):
        self.wsdiscovery = WSDiscoverySingleAdapter("Loopback Pseudo-Interface 1")
        self.wsdiscovery.start()
        self._locValidators = [pmtypes.InstanceIdentifier('Validator', extensionString='System')]

    def tearDown(self):
        self.wsdiscovery.stop()

    def setUpCocoDraft10(self):
        self.cocoFinalLocation = SdcLocation(fac='tklx', poc='CU1', bed='cocoDraft10Bed')

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
