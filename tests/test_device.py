import logging
import threading
import time
import unittest
import uuid

from sdc11073 import wsdiscovery
from sdc11073.xml_types import pm_qnames
from sdc11073.xml_types import pm_types
from sdc11073.xml_types import wsd_types

from tests import utils
from tests.mockstuff import SomeDevice


# pylint: disable=protected-access

class Test_Device(unittest.TestCase):

    def setUp(self):
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = wsdiscovery.WSDiscovery('127.0.0.1')
        self.wsd.start()
        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml')
        self.sdc_device.start_all()
        self._locValidators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.set_location(utils.random_location(), self._locValidators)

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



class Test_Device_2_mds(unittest.TestCase):

    def setUp(self):
        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = wsdiscovery.WSDiscovery('127.0.0.1')
        self.wsd.start()
        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, None, 'mdib_two_mds.xml')
        self.sdc_device.start_all()
        self._locValidators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]

        time.sleep(0.1)  # allow full init of device

        print('############### setUp done {} ##############'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))

    def tearDown(self):
        print('############### tearDown {}... ##############'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### tearDown {} ... ##############'.format(self._testMethodName))
        self.sdc_device.stop_all()
        self.wsd.stop()

    def test_set_location(self):
        """Call of set_location without giving a descriptor handle shall raise a ValueError."""
        # first make sure there is only one LocationContextDescriptor
        location_context_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(
            pm_qnames.LocationContextDescriptor)
        self.assertEqual(len(location_context_descriptors), 1)

        context_descriptor_handle = location_context_descriptors[0].Handle
        states_count = len(self.sdc_device.mdib.context_states.descriptor_handle.get(context_descriptor_handle, []))

        self.sdc_device.mdib.xtra.ensure_location_context_descriptor()  # this adds descriptor to 2nd mib
        # verify that there are now two LocationContextDescriptors
        location_context_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(
            pm_qnames.LocationContextDescriptor)
        self.assertEqual(len(location_context_descriptors), 2)

        self.assertRaises(ValueError, self.sdc_device.set_location, utils.random_location(), self._locValidators)

        # with descriptor handle it shall work
        self.sdc_device.set_location(utils.random_location(), self._locValidators,
                                     location_context_descriptor_handle=context_descriptor_handle)
        states2 = self.sdc_device.mdib.context_states.descriptor_handle.get(context_descriptor_handle)
        self.assertEqual(len(states2), states_count + 1)

    def test_ensure_patient_context_descriptor(self):
        """Verify that ensure_patient_context_descriptor creates the missing PatientContextDescriptor in 2nd mds."""
        patient_context_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(
            pm_qnames.PatientContextDescriptor)
        self.assertEqual(len(patient_context_descriptors), 1)
        self.sdc_device.mdib.xtra.ensure_patient_context_descriptor()
        patient_context_descriptors = self.sdc_device.mdib.descriptions.NODETYPE.get(
            pm_qnames.PatientContextDescriptor)
        self.assertEqual(len(patient_context_descriptors), 2)



class Test_Hello_And_Bye(unittest.TestCase):
    def test_send_hello_and_bye_at_start_and_stop(self):
        """
        Test whether the device does not send hello on initialization but on start and send bye on stop.
        """
        wait_for_callback = 3
        recv_hello = threading.Event()
        recv_bye = threading.Event()

        loc = utils.random_location()
        device_uuid = uuid.uuid4()

        def hello_callback(_, __):
            recv_hello.set()

        def bye_callback(_, epr):
            if epr == device_uuid.urn:
                recv_bye.set()

        wsd_device = wsdiscovery.WSDiscovery('127.0.0.1')
        wsd_device.start()
        sdc_device = SomeDevice.from_mdib_file(wsdiscovery=wsd_device,
                                               epr=device_uuid,
                                               mdib_xml_path='70041_MDIB_Final.xml')

        wsd_obj = wsdiscovery.WSDiscovery('127.0.0.1')

        wsd_obj.set_remote_service_hello_callback(callback=hello_callback,
                                                  scopes=wsd_types.ScopesType(value=loc.scope_string))
        wsd_obj.set_remote_service_bye_callback(callback=bye_callback)

        wsd_obj.start()

        self.assertFalse(recv_hello.wait(timeout=wait_for_callback))
        self.assertFalse(recv_bye.wait(timeout=wait_for_callback))

        sdc_device.start_all()
        _loc_validators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
        sdc_device.set_location(location=loc, validators=_loc_validators)

        self.assertTrue(recv_hello.wait(timeout=wait_for_callback))
        self.assertFalse(recv_bye.wait(timeout=wait_for_callback))

        sdc_device.stop_all()
        # Hint: the immediate call of wsd_device.stop() caused a dropping of Bye-messages
        # (in networkingthread.py not all items in _send_queue were processed - this is fixed and tested here)
        wsd_device.stop()

        self.assertTrue(recv_bye.wait(timeout=wait_for_callback))

        wsd_obj.stop()
