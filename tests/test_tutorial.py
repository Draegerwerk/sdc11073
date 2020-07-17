import unittest
import os
import uuid
import sdc11073


SEARCH_TIMEOUT = 2 # in real world applications this timeout is too short, 10 seconds is a good value.
                   # Here this short timeout is used to accelerate the test.

here = os.path.dirname(__file__)
my_mdib_path = os.path.join(here, '70041_MDIB_Final.xml')


def createGenericDevice(wsdiscovery_instance, location, mdibPath):
    my_mdib = sdc11073.mdib.DeviceMdibContainer.fromMdibFile(mdibPath)
    my_uuid = uuid.uuid4()
    dpwsModel = sdc11073.pysoap.soapenvelope.DPWSThisModel(manufacturer='Draeger',
                                                           manufacturerUrl='www.draeger.com',
                                                           modelName='TestDevice',
                                                           modelNumber='1.0',
                                                           modelUrl='www.draeger.com/model',
                                                           presentationUrl='www.draeger.com/model/presentation')

    dpwsDevice = sdc11073.pysoap.soapenvelope.DPWSThisDevice(friendlyName='TestDevice',
                                                             firmwareVersion='Version1',
                                                             serialNumber='12345')
    sdcDevice = sdc11073.sdcdevice.sdcdeviceimpl.PublishingSdcDevice(wsdiscovery_instance,
                                                                     my_uuid,
                                                                     dpwsModel,
                                                                     dpwsDevice,
                                                                     my_mdib)
    #sdcDevice._handler.mkDefaultRoleHandlers()
    for desc in sdcDevice.mdib.descriptions.objects:
        desc.SafetyClassification = sdc11073.pmtypes.SafetyClassification.MED_A
    sdcDevice.startAll(startRealtimeSampleLoop=False)    
    validators = [sdc11073.pmtypes.InstanceIdentifier('Validator', extensionString='System')]
    sdcDevice.setLocation(location, validators)
    return sdcDevice



class Test_Tutorial(unittest.TestCase):
    """ run tutorial examples as unit tests, so that broken examples are automatically detected"""

    def setUp(self) -> None:
        self.my_location = sdc11073.location.SdcLocation(fac='ODDS',
                                                         poc='CU1',
                                                         bed='BedSim')
        self.my_location2 = sdc11073.location.SdcLocation(fac='ODDS',
                                                          poc='CU2',
                                                          bed='BedSim')
        # tests fill these lists with what they create, teardown cleans up after them.
        self.my_devices = []
        self.my_clients = []
        self.my_wsdiscoveries = []

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

    def test_createDevice(self):
        # A WsDiscovery instance is needed to publish devices on the network.
        # In this case we want to publish them only on localhost 127.0.0.1.
        my_wsDiscovery = sdc11073.wsdiscovery.WSDiscoveryWhitelist(['127.0.0.1'])
        self.my_wsdiscoveries.append(my_wsDiscovery)
        my_wsDiscovery.start()

        # to create a device, this what you usually do:
        my_genericDevice = createGenericDevice(my_wsDiscovery, self.my_location, my_mdib_path)
        self.my_devices.append(my_genericDevice)

    def test_searchDevice(self):
        # create one discovery and two device that we can then search for
        my_wsDiscovery = sdc11073.wsdiscovery.WSDiscoveryWhitelist(['127.0.0.1'])
        self.my_wsdiscoveries.append(my_wsDiscovery)
        my_wsDiscovery.start()

        my_genericDevice1 = createGenericDevice(my_wsDiscovery, self.my_location, my_mdib_path)
        self.my_devices.append(my_genericDevice1)

        my_genericDevice2 = createGenericDevice(my_wsDiscovery, self.my_location2, my_mdib_path)
        self.my_devices.append(my_genericDevice2)

        # Search for devices
        # ------------------
        # create a new discovery instance for searching.
        # (technically this would not be necessary, but it makes things much clearer in our example)
        # for searching we use again localhost adapter. For demonstration purpose a WSDiscoverySingleAdapter is used
        my_client_wsDiscovery = sdc11073.wsdiscovery.WSDiscoverySingleAdapter('Loopback Pseudo-Interface 1')
        self.my_wsdiscoveries.append(my_client_wsDiscovery)
        my_client_wsDiscovery.start()

        # there a different methods to detect devices:
        # without specifying a type and a location, every WsDiscovery compatible device will be detected
        # (that can even be printers).
        services = my_client_wsDiscovery.searchServices(timeout=SEARCH_TIMEOUT)
        self.assertEqual(len(services), 2) # both devices found

        # now search only for devices in my_location2
        services = my_client_wsDiscovery.searchServices(scopes=[sdc11073.wsdiscovery.Scope(self.my_location2.scopeStringSdc)], timeout=SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)

        #search for medical devices only (BICEPS FInal version only)
        services = my_client_wsDiscovery.searchServices(types=sdc11073.definitions_sdc.SDC_v1_Definitions.MedicalDeviceTypesFilter, timeout=SEARCH_TIMEOUT)
        self.assertEqual(len(services), 2)

        #search for medical devices only all known protocol versions
        all_types = [p.MedicalDeviceTypesFilter for p in sdc11073.definitions_base.ProtocolsRegistry.protocols]
        services = my_client_wsDiscovery.searchMultipleTypes(typesList=all_types,
                                                        timeout=SEARCH_TIMEOUT)

        self.assertEqual(len(services), 2)

    def test_createClient(self):
        # create one discovery and one device that we can then search for
        my_wsDiscovery = sdc11073.wsdiscovery.WSDiscoveryWhitelist(['127.0.0.1'])
        self.my_wsdiscoveries.append(my_wsDiscovery)
        my_wsDiscovery.start()

        my_genericDevice1 = createGenericDevice(my_wsDiscovery, self.my_location, my_mdib_path)
        self.my_devices.append(my_genericDevice1)

        my_client_wsDiscovery = sdc11073.wsdiscovery.WSDiscoveryWhitelist(['127.0.0.1'])
        self.my_wsdiscoveries.append(my_client_wsDiscovery)
        my_client_wsDiscovery.start()

        # there a different methods to detect devices:
        # without specifying a type and a location, every WsDiscovery compatible device will be detected
        # (that can even be printers).
        services = my_client_wsDiscovery.searchServices(timeout=SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1) # both devices found

        my_client = sdc11073.sdcclient.SdcClient.fromWsdService(services[0])
        self.my_clients.append(my_client)
        my_client.startAll()
        ############# Mdib usage ##############################
        # In data oriented tests a mdib instance is very handy:
        # The mdib collects all data and makes it easily available for the test
        # The MdibContainer wraps data in "container" objects.
        # The basic idea is that every node that has a handle becomes directly accessible via its handle.
        myMdib = sdc11073.mdib.clientmdib.ClientMdibContainer(my_client)
        myMdib.initMdib() # myMdib keeps itself now updated

        # now query some data
        # mdib has three lookups: descriptions, states and contextStates
        # each lookup can be searched by different keeys,
        # e.g looking for a descriptor by type looks like this:
        locationContextDescriptorContainers = myMdib.descriptions.NODETYPE.get(sdc11073.namespaces.domTag('LocationContextDescriptor'))
        self.assertEqual(len(locationContextDescriptorContainers), 1)
        # we can look for the corresponding state by handle:
        locationContextStateContainers = myMdib.contextStates.descriptorHandle.get(locationContextDescriptorContainers[0].handle)
        self.assertEqual(len(locationContextStateContainers), 1)

    def test_callOperation(self):
        # create one discovery and one device that we can then search for
        my_wsDiscovery = sdc11073.wsdiscovery.WSDiscoveryWhitelist(['127.0.0.1'])
        self.my_wsdiscoveries.append(my_wsDiscovery)
        my_wsDiscovery.start()

        my_genericDevice1 = createGenericDevice(my_wsDiscovery, self.my_location, my_mdib_path)
        self.my_devices.append(my_genericDevice1)

        my_client_wsDiscovery = sdc11073.wsdiscovery.WSDiscoveryWhitelist(['127.0.0.1'])
        self.my_wsdiscoveries.append(my_client_wsDiscovery)
        my_client_wsDiscovery.start()

        # there a different methods to detect devices:
        # without specifying a type and a location, every WsDiscovery compatible device will be detected
        # (that can even be printers).
        services = my_client_wsDiscovery.searchServices(timeout=SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1) # both devices found

        my_client = sdc11073.sdcclient.SdcClient.fromWsdService(services[0])
        self.my_clients.append(my_client)
        my_client.startAll()
        myMdib = sdc11073.mdib.clientmdib.ClientMdibContainer(my_client)
        myMdib.initMdib()

        # we want to set a patient.
        # first we must find the operation that has PatientContextDescriptor as operation target
        patientContextDescriptorContainers = myMdib.descriptions.NODETYPE.get(sdc11073.namespaces.domTag('PatientContextDescriptor'))
        self.assertEqual(len(patientContextDescriptorContainers), 1)
        myPatientContextDescriptorContainer = patientContextDescriptorContainers[0]
        all_operations = myMdib.descriptions.NODETYPE.get(sdc11073.namespaces.domTag('SetContextStateOperationDescriptor'), [])
        my_operations = [op for op in all_operations if op.OperationTarget == myPatientContextDescriptorContainer.handle]
        self.assertEqual(len(my_operations), 1)
        my_operation = my_operations[0]

        # make a proposed patient context:
        contextService = my_client.ContextService_client
        proposedPatient = contextService.mkProposedContextObject(myPatientContextDescriptorContainer.handle)
        proposedPatient.Firstname = 'Jack'
        proposedPatient.Lastname = 'Miller'
        future = contextService.setContextState(operationHandle=my_operation.handle,
                                                proposedContextStates=[proposedPatient])
        result = future.result(timeout=5)
        self.assertEqual(result.state, sdc11073.pmtypes.InvocationState.FINISHED)
