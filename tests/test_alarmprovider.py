import logging
import sys
import time
import unittest
from unittest import mock

from sdc11073 import commlog
from sdc11073 import loghelper
from sdc11073 import pmtypes
from sdc11073.mdib.clientmdib import ClientMdibContainer
from sdc11073.sdcclient import SdcClient
from sdc11073.wsdiscovery import WSDiscoveryWhitelist
from tests import utils
from tests.mockstuff import SomeDevice

ENABLE_COMMLOG = False
if ENABLE_COMMLOG:
    commLogger = commlog.CommLogger(log_folder=r'c:\temp\sdc_commlog',
                                    log_out=True,
                                    log_in=True,
                                    broadcastIpFilter=None)
    commlog.defaultLogger = commLogger

CLIENT_VALIDATE = True
SET_TIMEOUT = 10  # longer timeout than usually needed, but jenkins jobs frequently failed with 3 seconds timeout
NOTIFICATION_TIMEOUT = 5  # also jenkins related value


class Test_Client_SomeDevice_AlertDelegate(unittest.TestCase):
    """This is a test with a mdib that allows alert delegation and a role-provider that implements alert delegation
    acc. to BICEPS chapter 6.2"""

    def setUp(self):
        sys.stderr.write('\n############### start setUp {} ##############\n'.format(self._testMethodName))

        logging.getLogger('sdc').info('############### start setUp {} ##############'.format(self._testMethodName))
        self.wsd = WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        location = utils.random_location()
        my_uuid = None  # let device create one
        self.sdc_device = SomeDevice.fromMdibFile(self.wsd, my_uuid, 'mdib_tns.xml', logLevel=logging.INFO)

        self.sdc_device.startAll()
        self._loc_validators = [pmtypes.InstanceIdentifier('Validator', extensionString='System')]
        self.sdc_device.setLocation(location, self._loc_validators)

        time.sleep(0.5)  # allow full init of devices

        xAddr = self.sdc_device.getXAddrs()
        self.sdc_client = SdcClient(xAddr[0],
                                    deviceType=self.sdc_device.mdib.sdc_definitions.MedicalDeviceType,
                                    validate=CLIENT_VALIDATE)

        self.sdc_client.startAll()

        time.sleep(1)
        sys.stderr.write('\n############### setUp done {} ##############\n'.format(self._testMethodName))
        logging.getLogger('sdc').info('############### setUp done {} ##############'.format(self._testMethodName))
        time.sleep(0.5)
        self.log_watcher = loghelper.LogWatcher(logging.getLogger('sdc'), level=logging.ERROR)

    def tearDown(self):
        sys.stderr.write('############### tearDown {}... ##############\n'.format(self._testMethodName))
        self.log_watcher.setPaused(True)
        self.sdc_client.stopAll()
        self.sdc_device.stopAll()
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
        cl_mdib.initMdib()
        all_cl_handles = set(cl_mdib.descriptions.handle.keys())
        all_dev_handles = set(self.sdc_device.mdib.descriptions.handle.keys())
        self.assertEqual(all_cl_handles, all_dev_handles)
        self.assertEqual(len(cl_mdib.states.objects), len(self.sdc_device.mdib.states.objects))

    def test_delegate(self):
        cl_mdib = ClientMdibContainer(self.sdc_client)
        cl_mdib.initMdib()
        # set an alarm condition and start local signal
        with self.sdc_device.mdib.mdibUpdateTransaction() as mgr:
            alert_condition_state = mgr.getAlertState('ac0.mds0')
            alert_condition_state.ActivationState = pmtypes.AlertActivation.ON
            alert_condition_state.Presence = True
            local_alert_signal_state = mgr.getAlertState('as0.mds0')
            local_alert_signal_state.ActivationState = pmtypes.AlertActivation.ON
            local_alert_signal_state.Presence = pmtypes.AlertSignalPresence.ON
        # verify that remote signal is still off
        remote_alert_signal_state = self.sdc_device.mdib.states.descriptorHandle.getOne('as0.mds0_rem')
        self.assertEqual(pmtypes.AlertSignalPresence.OFF, remote_alert_signal_state.Presence)
        self.assertEqual(pmtypes.AlertActivation.OFF, remote_alert_signal_state.ActivationState)

        # call activate method for delegate all alarms
        proposed_alert_state = cl_mdib.mkProposedState('as0.mds0_rem')
        proposed_alert_state.ActivationState = pmtypes.AlertActivation.ON
        proposed_alert_state.Presence = pmtypes.AlertSignalPresence.ON
        future = self.sdc_client.SetService_client.setAlertState('as0.mds0_rem_dele', proposed_alert_state)
        operation_result = future.result()
        self.assertEqual('Fin', operation_result.state, msg=f'state={operation_result.state}, error={operation_result.error} msg={operation_result.errorMsg}')

        # verify that now remote signal in on and local signal is off
        local_alert_signal_state = cl_mdib.states.descriptorHandle.getOne('as0.mds0')
        remote_alert_signal_state = cl_mdib.states.descriptorHandle.getOne('as0.mds0_rem')
        self.assertEqual(pmtypes.AlertActivation.PAUSED, local_alert_signal_state.ActivationState)
        self.assertEqual(pmtypes.AlertActivation.ON, remote_alert_signal_state.ActivationState)
        time.sleep(5)
        self.assertEqual(pmtypes.AlertActivation.ON, local_alert_signal_state.ActivationState)
        self.assertEqual(pmtypes.AlertActivation.OFF, remote_alert_signal_state.ActivationState)

    def test_current_alerts(self):
        """Test AlertSystemState handling.

         Verify that alarm provider sets alarm condition lists in AlertSystemState.
         Verify that alarm provider periodically updates AlertSystemState.
         """
        mds_alert_system_state0 = self.sdc_device.mdib.states.descriptorHandle.getOne('asy.mds0')
        # verify that initially the list of current alerts is empty
        self.assertEqual(0, len(mds_alert_system_state0.PresentTechnicalAlarmConditions))
        # AlertConditionState ac0.mds0 is ActivationState="On" and Presence="true" in initial mdib
        # =>there is already one alert state in list
        self.assertEqual(1, len(mds_alert_system_state0.PresentPhysiologicalAlarmConditions))
        # activate technical alarm
        with self.sdc_device.mdib.mdibUpdateTransaction() as mgr:
            alert_condition_state = mgr.getAlertState('ac1.mds0')
            alert_condition_state.ActivationState = pmtypes.AlertActivation.ON
            alert_condition_state.Presence = True
        mds_alert_system_state1 = self.sdc_device.mdib.states.descriptorHandle.getOne('asy.mds0')
        self.assertEqual(1, len(mds_alert_system_state1.PresentTechnicalAlarmConditions))

        # verify that alarm role provider sends alert system state periodically (self check)
        # check both alert systems im mdib
        vmd_alert_system_state1 = self.sdc_device.mdib.states.descriptorHandle.getOne('asy.vmd0')

        time.sleep(6)  # self check period is 5 seconds, at least one self check must have happened
        mds_alert_system_state2 = self.sdc_device.mdib.states.descriptorHandle.getOne('asy.mds0')
        vmd_alert_system_state2 = self.sdc_device.mdib.states.descriptorHandle.getOne('asy.vmd0')
        self.assertGreater(vmd_alert_system_state2.StateVersion, vmd_alert_system_state1.StateVersion)
        self.assertGreater(mds_alert_system_state2.StateVersion, mds_alert_system_state1.StateVersion)
        self.assertGreater(vmd_alert_system_state2.SelfCheckCount, vmd_alert_system_state1.SelfCheckCount)
        self.assertGreater(mds_alert_system_state2.SelfCheckCount, mds_alert_system_state1.SelfCheckCount)

        # verify that self check logs an error if something fails
        self.log_watcher.setPaused(True)  # deactivate the main log watcher because the following will cause errors
        tmp_log_watcher = loghelper.LogWatcher(logging.getLogger('sdc.device.GenericAlarmProvider'),
                                               level=logging.ERROR)
        my_mock = mock.MagicMock(side_effect = Exception('boom!'))
        with mock.patch.object(self.sdc_device.mdib, 'mdibUpdateTransaction', my_mock):
            time.sleep(3)
        records = tmp_log_watcher.getAllRecords()
        self.assertGreater(len(records), 0)
        for record in records:
            self.assertTrue('boom!' in record.record.message)

