import unittest
import os
import uuid
import threading
import traceback
import time
from concurrent import futures
from collections import defaultdict
import sdc11073
from sdc11073.definitions_sdc import SDC_v1_Definitions

here = os.path.dirname(__file__)
default_mdib_path = os.path.join(here, 'reference_mdib.xml')
mdib_path = os.getenv('ref_mdib') or default_mdib_path

My_Dev_UUID_str = '12345678-6f55-11ea-9697-123456789abc'

# these variables define how the device is published on the network and how the client tries to detect the device:
adapter_ip = os.getenv('ref_ip') or '127.0.0.1'
ref_fac = os.getenv('ref_fac') or 'r_fac'
ref_poc = os.getenv('ref_poc') or 'r_poc'
ref_bed = os.getenv('ref_bed') or 'r_bed'


class DeviceActivity(threading.Thread):
    """ This thread feeds the device with periodic updates of metrics and alert states"""
    daemon = True
    def __init__(self, device):
        super().__init__()
        self.device = device
        self.running = None

    def run(self):
        self.running = True
        descs = list(self.device.mdib.descriptions.objects)
        descs.sort(key=lambda x: x.handle)
        metric = None
        alertCondition = None
        stringOperation = None
        valueOperation = None
        for oneContainer in descs:
            if oneContainer.handle == "numeric.ch1.vmd0":
                metric = oneContainer
            if oneContainer.handle == "ac0.mds0":
                alertCondition = oneContainer
            if oneContainer.handle == "numeric.ch0.vmd1_sco_0":
                valueOperation = oneContainer
            if oneContainer.handle == "enumstring.ch0.vmd1_sco_0":
                stringOperation = oneContainer
        with self.device.mdib.mdibUpdateTransaction() as mgr:
            state = mgr.getMetricState(valueOperation.OperationTarget)
            if not state.metricValue:
                state.mkMetricValue()
            state = mgr.getMetricState(stringOperation.OperationTarget)
            if not state.metricValue:
                state.mkMetricValue()
        print("DeviceActivity running...")
        try:
            currentValue = 0
            while True:
                if metric:
                    with self.device.mdib.mdibUpdateTransaction() as mgr:
                        state = mgr.getMetricState(metric.handle)
                        if not state.metricValue:
                            state.mkMetricValue()
                        state.metricValue.Value = currentValue
                        print ('set metric to {}'.format(currentValue))
                        currentValue += 1
                else:
                    print("Metric not found in MDIB!")
                if alertCondition:
                    with self.device.mdib.mdibUpdateTransaction() as mgr:
                        state = mgr.getAlertState(alertCondition.handle)
                        state.Presence = not state.Presence
                        print ('set alertstate presence to {}'.format(state.Presence))
                else:
                    print("Alert not found in MDIB")
                for _ in range(2):
                    if not self.running:
                        print("DeviceActivity stopped.")
                        return
                    else:
                        time.sleep(1)
        except :
            print(traceback.format_exc())
        print("DeviceActivity stopped.")


def createReferenceDevice(wsdiscovery_instance, location, mdibPath):
    my_mdib = sdc11073.mdib.DeviceMdibContainer.fromMdibFile(mdibPath)
    my_uuid = uuid.UUID(My_Dev_UUID_str)
    dpwsModel = sdc11073.pysoap.soapenvelope.DPWSThisModel(manufacturer='sdc11073',
                                                        manufacturerUrl='www.sdc11073.com',
                                                        modelName='TestDevice',
                                                        modelNumber='1.0',
                                                        modelUrl='www.draeger.com/model',
                                                        presentationUrl='www.draeger.com/model/presentation')

    dpwsDevice = sdc11073.pysoap.soapenvelope.DPWSThisDevice(friendlyName='TestDevice',
                                                          firmwareVersion='Version1',
                                                          serialNumber='12345')
    sdcDevice = sdc11073.sdcdevice.sdcdeviceimpl.SdcDevice(wsdiscovery_instance,
                                                           my_uuid,
                                                           dpwsModel,
                                                           dpwsDevice,
                                                           my_mdib)
    for desc in sdcDevice.mdib.descriptions.objects:
        desc.SafetyClassification = sdc11073.pmtypes.SafetyClassification.MED_A
    sdcDevice.startAll(startRealtimeSampleLoop=False)
    validators = [sdc11073.pmtypes.InstanceIdentifier('Validator', extensionString='System')]
    sdcDevice.setLocation(location, validators)

    patientDescriptorHandle = my_mdib.descriptions.nodeName.get(sdc11073.namespaces.domTag('PatientContext'))[0].handle
    with my_mdib.mdibUpdateTransaction() as mgr:
        patientContainer = mgr.getContextState(patientDescriptorHandle)
        patientContainer.Givenname = "Given"
        patientContainer.Middlename = "Middle"
        patientContainer.Familyname = "Familiy"
        patientContainer.Birthname = "Birthname"
        patientContainer.Title = "Title"
        patientContainer.ContextAssociation = "Assoc"
        identifiers = []
        patientContainer.Identification = identifiers

    return sdcDevice


class Test_Reference(unittest.TestCase):
    """Plugfest Reference tests"""
    def setUp(self) -> None:
        self.my_location = sdc11073.location.SdcLocation(fac=ref_fac,
                                                         poc=ref_poc,
                                                         bed=ref_bed)
        # tests fill these lists with what they create, teardown cleans up after them.
        self.my_devices = []
        self.my_clients = []
        self.my_wsdiscoveries = []
        self.device_activity = None

    def tearDown(self) -> None:
        for cl in self.my_clients:
            print('stopping {}'.format(cl))
            cl.stopAll()
        for d in self.my_devices:
            print('stopping {}'.format(d))
            d.stopAll()
        for w in self.my_wsdiscoveries:
            print('stopping {}'.format(w))
            w.stop()
        if self.device_activity:
            self.device_activity.running = False
            self.device_activity.join()

    def test_with_created_device(self):
        # This test creates its own device and runs the tests against it
        # A WsDiscovery instance is needed to publish devices on the network.
        # In this case we want to publish them only on localhost 127.0.0.1.
        my_device_wsDiscovery = sdc11073.wsdiscovery.WSDiscoveryWhitelist([adapter_ip])
        self.my_wsdiscoveries.append(my_device_wsDiscovery)
        my_device_wsDiscovery.start()

        # generic way to create a device, this what you usually do:
        my_genericDevice = createReferenceDevice(my_device_wsDiscovery, self.my_location, mdib_path)
        self.my_devices.append(my_genericDevice)
        self.device_activity = DeviceActivity(my_genericDevice)
        self.device_activity.start()
        time.sleep(1)
        self._runtest_client_connects()

    @unittest.skip
    def test_client_connects(self):
        # This test need an externally started device to run the tests against it.
        #
        self._runtest_client_connects()

    def _runtest_client_connects(self):
        """sequence of client actions"""
        errors = []
        passed = []
        my_client_wsDiscovery = sdc11073.wsdiscovery.WSDiscoveryWhitelist([adapter_ip])
        self.my_wsdiscoveries.append(my_client_wsDiscovery)
        my_client_wsDiscovery.start()

        print ('looking for device with scope {}'.format(self.my_location.scopeStringSdc))
        services = my_client_wsDiscovery.searchServices(types=SDC_v1_Definitions.MedicalDeviceTypesFilter,
                                                        scopes=[sdc11073.wsdiscovery.Scope(self.my_location.scopeStringSdc)])
        print('found {} services {}'.format(len(services), ', '.join([s.getEPR() for s in services])))
        for s in services:
            print(s.getEPR())
        self.assertEqual(len(services),1)
        my_service = services[0]
        print('Test step 1 successful: device discovered')

        print('Test step 2: connect to device...')
        client = sdc11073.sdcclient.SdcClient.fromWsdService(my_service)
        self.my_clients.append(client)
        client.startAll()
        self.assertTrue(client.isConnected)
        print('Test step 2 successful: connected to device')

        print('Test step 3&4: get mdib and subscribe...')
        mdib = sdc11073.mdib.clientmdib.ClientMdibContainer(client)
        mdib.initMdib()
        self.assertGreater(len(mdib.descriptions.objects), 0) # at least one descriptor
        self.assertTrue(client._subscriptionMgr.allSubscriptionsOkay) # at least one descriptor

        # we want to exec. ALL following steps, therefore collect data and do test at the end.
        print('Test step 5: check that at least one patient context exists')
        patients = mdib.contextStates.NODETYPE.get(sdc11073.namespaces.domTag('PatientContextState'), [])
        if not patients:
            errors.append('### Test 5 ### failed')

        print('Test step 6: check that at least one location context exists')
        locations = mdib.contextStates.NODETYPE.get(sdc11073.namespaces.domTag('LocationContextState'), [])
        if not locations:
            errors.append('### Test 6 ### failed')
        _passed, _errors = self._test_state_updates(mdib)
        errors.extend(_errors)
        passed.extend(_passed)
        _passed, _errors = self._test_setstring_operation(mdib, client)
        errors.extend(_errors)
        passed.extend(_passed)
        _passed, _errors = self._test_setvalue_operation(mdib, client)
        errors.extend(_errors)
        passed.extend(_passed)
        print(errors)
        print(passed)
        self.assertEqual(len(errors), 0, msg='expected no Errors, got:{}'.format(', '.join(errors)))
        self.assertEqual(len(passed), 4, msg='expected 4 Passed, got :{}'.format(', '.join(passed)))

    def _test_state_updates(self, mdib):
        passed = []
        errors = []
        print('Test step 7&8: count metric state updates and alert state updates')
        metric_updates = defaultdict(list)
        alert_updates = defaultdict(list)

        def onMetricUpdates(metricsbyhandle):
            print('onMetricUpdates', metricsbyhandle)
            for k, v in metricsbyhandle.items():
                metric_updates[k].append(v)

        def onAlertUpdates(alertsbyhandle):
            print('onAlertUpdates', alertsbyhandle)
            for k, v in alertsbyhandle.items():
                alert_updates[k].append(v)

        sdc11073.observableproperties.bind(mdib, metricsByHandle=onMetricUpdates)
        sdc11073.observableproperties.bind(mdib, alertByHandle=onAlertUpdates)

        sleep_timer = 11
        min_updates = sleep_timer // 5 - 1
        print('will wait for {} seconds now, expecting at least {} updates per handle'.format(sleep_timer,
                                                                                              metric_updates))
        time.sleep(sleep_timer)
        print(metric_updates)
        print(alert_updates)
        found_error = False
        if not(metric_updates):
            print('found no metric state updates at all, test step 8 failed')
            found_error = True
        for k, v in metric_updates.items():
            if len(v) < min_updates:
                print('found only {} updates for {}, test step 7 failed'.format(len(v), k))
                found_error = True
            else:
                print('found {} updates for {}, test step 7 ok'.format(len(v), k))
        if found_error:
            errors.append('### Test 7 ### failed')
        else:
            passed.append('### Test 7 ### passed')

        found_error = False
        if not(alert_updates):
            print('found no alert state updates at all, test step 8 failed')
            found_error = True
        for k, v in alert_updates.items():
            if len(v) < min_updates:
                print('found only {} updates for {}, test step 8 failed'.format(len(v), k))
            else:
                print('found {} updates for {}, test step 8 ok'.format(len(v), k))
        if found_error:
            errors.append('### Test 8 ### failed')
        else:
            passed.append('### Test 8 ### passed')
        return passed, errors

    def _test_setstring_operation(self, mdib, client):
        passed = []
        errors = []
        print('Test step 9: call SetString operation')
        setstring_operations = mdib.descriptions.NODETYPE.get(sdc11073.namespaces.domTag('SetStringOperationDescriptor'),
                                                              [])
        setst_handle = 'string.ch0.vmd1_sco_0'
        if len(setstring_operations) == 0:
            print('Test step 9 failed, no SetString operation found')
            errors.append('### Test 9 ### failed')
        else:
            for s in setstring_operations:
                if s.handle != setst_handle:
                    continue
                print('setString Op ={}'.format(s))
                fut = client.SetService_client.setString(s.handle, 'hoppeldipop')
                try:
                    res = fut.result(timeout=10)
                    print(res)
                    if res.state != sdc11073.pmtypes.InvocationState.FINISHED:
                        print('set string operation {} did not finish with "Fin":{}'.format(s.handle, res))
                        errors.append('### Test 9 ### failed')
                    else:
                        print('set value operation {} ok:{}'.format(s.handle, res))
                        passed.append('### Test 9 ### passed')
                except futures.TimeoutError:
                    print('timeout error')
                    errors.append('### Test 9 ### failed')
        return passed, errors

    def _test_setvalue_operation(self, mdib, client):
        passed = []
        errors = []
        print('Test step 10: call SetValue operation')
        setvalue_operations = mdib.descriptions.NODETYPE.get(sdc11073.namespaces.domTag('SetValueOperationDescriptor'), [])
        #    print('setvalue_operations', setvalue_operations)
        setval_handle = 'numeric.ch0.vmd1_sco_0'
        if len(setvalue_operations) == 0:
            print('Test step 10 failed, no SetValue operation found')
            errors.append('### Test 10 ### failed')
        else:
            for s in setvalue_operations:
                if s.handle != setval_handle:
                    continue
                print('setNumericValue Op ={}'.format(s))
                fut = client.SetService_client.setNumericValue(s.handle, 42)
                try:
                    res = fut.result(timeout=10)
                    print(res)
                    if res.state != sdc11073.pmtypes.InvocationState.FINISHED:
                        print('set value operation {} did not finish with "Fin":{}'.format(s.handle, res))
                    else:
                        print('set value operation {} ok:{}'.format(s.handle, res))
                    passed.append('### Test 10 ### passed')
                except futures.TimeoutError:
                    print('timeout error')
                    errors.append('### Test 10 ### failed')
        return passed, errors

