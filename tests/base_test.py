import unittest
import os
from sdc11073.wsdiscovery import WSDiscoverySingleAdapter
from sdc11073 import pmtypes
from sdc11073.location import SdcLocation
from sdc11073.sdcclient import SdcClient
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
        self._locValidators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]

    def tearDown(self):
        self.wsdiscovery.stop()

    def setUpCocoDraft10(self):
        self.cocoFinalLocation = SdcLocation(fac='tklx', poc='CU1', bed='cocoDraft10Bed')

        self.sdcDeviceCoCoFinal = SomeDevice.from_mdib_file(self.wsdiscovery, None, '70041_MDIB_Final.xml')
        self.sdcDeviceCoCoFinal.start_all()
        self.sdcDeviceCoCoFinal.set_location(self.cocoFinalLocation, self._locValidators)
        xAddr = self.sdcDeviceCoCoFinal.get_xaddrs()
        self.sdcClientCocoFinal = SdcClient(xAddr[0],
                                            deviceType=self.sdcDeviceCoCoFinal.mdib.sdc_definitions.MedicalDeviceType,
                                            validate=True)
        self.sdcClientCocoFinal.start_all()

    def stopDraft10(self):
        self.sdcClientCocoFinal.stop_all()
        self.sdcDeviceCoCoFinal.stop_all()
