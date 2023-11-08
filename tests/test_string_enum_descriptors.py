import logging
import sys
import time
import unittest

from sdc11073 import commlog
from sdc11073 import loghelper
from sdc11073.consumer import SdcConsumer
from sdc11073.mdib.consumermdib import ConsumerMdib
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_types import pm_types
from sdc11073.xml_types.actions import periodic_actions
from tests import utils
from tests.mockstuff import SomeDevice

ENABLE_COMMLOG = False

comm_logger = commlog.DirectoryLogger(log_folder=r'c:\temp\sdc_commlog',
                                      log_out=True,
                                      log_in=True,
                                      broadcast_ip_filter=None)

CLIENT_VALIDATE = True
SET_TIMEOUT = 10  # longer timeout than usually needed, but jenkins jobs frequently failed with 3 seconds timeout
NOTIFICATION_TIMEOUT = 5  # also jenkins related value


class Test_Client_SomeDevice_StringEnumDescriptors(unittest.TestCase):
    """This is a test that checks empty value for AllowedValue in state"""

    def setUp(self):
        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))

        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        if ENABLE_COMMLOG:
            comm_logger.start()
        self.wsd = WSDiscovery('127.0.0.1')
        self.wsd.start()
        my_uuid = None  # let device create one
        # self.sdc_device = SomeDevice.from_mdib_file(self.wsd, my_uuid, 'mdib_tns.xml')
        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, my_uuid, 'mdib_two_mds.xml')

        self.sdc_device.start_all()
        self._loc_validators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.set_location(utils.random_location(), self._loc_validators)

        time.sleep(0.5)  # allow full init of devices

        x_addr = self.sdc_device.get_xaddrs()
        self.sdc_client = SdcConsumer(x_addr[0],
                                      sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                      ssl_context_container=None,
                                      validate=CLIENT_VALIDATE)

        self.sdc_client.start_all(not_subscribed_actions=periodic_actions)

        time.sleep(1)
        sys.stderr.write('\n############### setUp done {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))
        time.sleep(0.5)
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def tearDown(self):
        sys.stderr.write('############### tearDown {}... ##############\n'.format(self._testMethodName))
        self.log_watcher.setPaused(True)
        self.sdc_client.stop_all()
        self.sdc_device.stop_all()
        self.wsd.stop()
        try:
            self.log_watcher.check()
        except loghelper.LogWatchError as ex:
            sys.stderr.write(repr(ex))
            raise
        comm_logger.stop()
        sys.stderr.write('############### tearDown {} done ##############\n'.format(self._testMethodName))

    def test_BasicConnect(self):
        # simply check that all descriptors are available in client after init_mdib
        cl_mdib = ConsumerMdib(self.sdc_client)
        cl_mdib.init_mdib()
        all_cl_handles = set(cl_mdib.descriptions.handle.keys())
        all_dev_handles = set(self.sdc_device.mdib.descriptions.handle.keys())
        self.assertEqual(all_cl_handles, all_dev_handles)
        self.assertEqual(len(cl_mdib.states.objects), len(self.sdc_device.mdib.states.objects))

    def test_allowed_values(self):
        cl_mdib = ConsumerMdib(self.sdc_client)
        cl_mdib.init_mdib()
        descr_handle = 'enumstring.ch0.vmd0'  # this has an empty string as AllowedValue.Value in enumeration
        # set an alarm condition and start local signal
        enum_descr = cl_mdib.descriptions.handle.get_one(descr_handle)
        for allowed_value in enum_descr.AllowedValue:
            with self.sdc_device.mdib.transaction_manager() as mgr:
                enum_state = mgr.get_state(descr_handle)
                enum_state.MetricValue.Value = allowed_value.Value
            time.sleep(1)
            received_state = cl_mdib.states.descriptor_handle.get_one(descr_handle)
            self.assertEqual(allowed_value.Value, received_state.MetricValue.Value)
