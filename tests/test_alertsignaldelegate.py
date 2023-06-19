import logging
import sys
import time
import unittest

from sdc11073 import commlog
from sdc11073 import loghelper
from sdc11073.xml_types import pm_types, msg_types
from sdc11073.location import SdcLocation
from sdc11073.loghelper import basic_logging_setup
from sdc11073.mdib.clientmdib import ClientMdibContainer
from sdc11073.sdcclient import SdcClient
from sdc11073.wsdiscovery import WSDiscovery
from tests.mockstuff import SomeDevice

ENABLE_COMMLOG = False
if ENABLE_COMMLOG:
    commLogger = commlog.CommLogger(log_folder=r'c:\temp\sdc_commlog',
                                    log_out=True,
                                    log_in=True,
                                    broadcast_ip_filter=None)
    commlog.defaultLogger = commLogger

CLIENT_VALIDATE = True
SET_TIMEOUT = 10  # longer timeout than usually needed, but jenkins jobs frequently failed with 3 seconds timeout
NOTIFICATION_TIMEOUT = 5  # also jenkins related value


class Test_Client_SomeDevice_AlertDelegate(unittest.TestCase):
    """This is a test with a mdib that allows alert delegation and a role-provider that implements alert delegation
    acc. to BICEPS chapter 6.2"""

    def setUp(self):
        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))
        basic_logging_setup()

        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = WSDiscovery('127.0.0.1')
        self.wsd.start()
        location = SdcLocation(fac='tklx', poc='CU1', bed='Bed')
        my_uuid = None  # let device create one
        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, my_uuid, 'mdib_two_mds.xml', log_prefix='<device> ')
        self.sdc_device.start_all()
        self._loc_validators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
        self.sdc_device.mdib.xtra.ensure_location_context_descriptor()
        self.sdc_device.set_location(location, self._loc_validators)
        # self.provideRealtimeData(self.sdc_device)

        time.sleep(0.5)  # allow full init of devices

        xAddr = self.sdc_device.get_xaddrs()
        self.sdc_client = SdcClient(xAddr[0],
                                    sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                    ssl_context=None,
                                    validate=CLIENT_VALIDATE,
                                    log_prefix='<client> ')
        self.sdc_client.start_all()

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
        except loghelper.LogWatchException as ex:
            sys.stderr.write(repr(ex))
            raise
        sys.stderr.write('############### tearDown {} done ##############\n'.format(self._testMethodName))

    def test_BasicConnect(self):
        # simply check that all descriptors are available in client after init_mdib
        cl_mdib = ClientMdibContainer(self.sdc_client)
        cl_mdib.init_mdib()
        all_cl_handles = set(cl_mdib.descriptions.handle.keys())
        all_dev_handles = set(self.sdc_device.mdib.descriptions.handle.keys())
        self.assertEqual(all_cl_handles, all_dev_handles)
        self.assertEqual(len(cl_mdib.states.objects), len(self.sdc_device.mdib.states.objects))

    def test_delegate(self):
        cl_mdib = ClientMdibContainer(self.sdc_client)
        cl_mdib.init_mdib()
        # set an alarm condition and start local signal
        with self.sdc_device.mdib.transaction_manager() as mgr:
            alert_condition_state = mgr.get_state('ac0.mds0')
            alert_condition_state.ActivationState = pm_types.AlertActivation.ON
            alert_condition_state.Presence = True
            local_alert_signal_state = mgr.get_state('as0.mds0')
            local_alert_signal_state.ActivationState = pm_types.AlertActivation.ON
            local_alert_signal_state.Presence = pm_types.AlertSignalPresence.ON
        # verify that remote signal is still off
        remote_alert_signal_state = self.sdc_device.mdib.states.descriptorHandle.get_one('as0.mds0_rem')
        self.assertEqual(pm_types.AlertSignalPresence.OFF, remote_alert_signal_state.Presence)
        self.assertEqual(pm_types.AlertActivation.OFF, remote_alert_signal_state.ActivationState)

        # call activate method for delegate all alarms
        proposed_alert_state = cl_mdib.xtra.mk_proposed_state('as0.mds0_rem')
        proposed_alert_state.ActivationState = pm_types.AlertActivation.ON
        proposed_alert_state.Presence = pm_types.AlertSignalPresence.ON
        future = self.sdc_client.set_service_client.set_alert_state('as0.mds0_rem_dele', proposed_alert_state)

        result = future.result(timeout=SET_TIMEOUT)
        state = result.InvocationInfo.InvocationState
        self.assertEqual(state, msg_types.InvocationState.FINISHED)

        # verify that now remote signal in on and local signal is off
        local_alert_signal_state = cl_mdib.states.descriptorHandle.get_one('as0.mds0')
        remote_alert_signal_state = cl_mdib.states.descriptorHandle.get_one('as0.mds0_rem')
        self.assertEqual(pm_types.AlertActivation.PAUSED, local_alert_signal_state.ActivationState)
        self.assertEqual(pm_types.AlertActivation.ON, remote_alert_signal_state.ActivationState)
        time.sleep(5)
        self.assertEqual(pm_types.AlertActivation.ON, local_alert_signal_state.ActivationState)
        self.assertEqual(pm_types.AlertActivation.OFF, remote_alert_signal_state.ActivationState)
