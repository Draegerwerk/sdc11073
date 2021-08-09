import os
import unittest
import uuid

from sdc11073 import pmtypes
from sdc11073.definitions_base import ProtocolsRegistry
from sdc11073.definitions_sdc import SDC_v1_Definitions
from sdc11073.location import SdcLocation
from sdc11073.mdib import DeviceMdibContainer
from sdc11073.mdib.clientmdib import ClientMdibContainer
from sdc11073.namespaces import domTag
from sdc11073.pmtypes import CodedValue
from sdc11073.pysoap.soapenvelope import DPWSThisModel, DPWSThisDevice
from sdc11073.roles.product import BaseProduct
from sdc11073.roles.providerbase import ProviderRole
from sdc11073.sdcclient import SdcClient
from sdc11073.sdcdevice.sdcdeviceimpl import SdcDevice
from sdc11073.wsdiscovery import WSDiscoveryWhitelist, WSDiscoverySingleAdapter, Scope

loopback_adapter = 'Loopback Pseudo-Interface 1' if os.name == 'nt' else 'lo'

SEARCH_TIMEOUT = 2  # in real world applications this timeout is too short, 10 seconds is a good value.
# Here this short timeout is used to accelerate the test.

here = os.path.dirname(__file__)
my_mdib_path = os.path.join(here, '70041_MDIB_Final.xml')


def createGenericDevice(wsdiscovery_instance, location, mdibPath, role_provider=None):
    my_mdib = DeviceMdibContainer.from_mdib_file(mdibPath)
    my_uuid = uuid.uuid4()
    dpwsModel = DPWSThisModel(manufacturer='Draeger',
                              manufacturer_url='www.draeger.com',
                              model_name='TestDevice',
                              model_number='1.0',
                              model_url='www.draeger.com/model',
                              presentation_url='www.draeger.com/model/presentation')

    dpwsDevice = DPWSThisDevice(friendly_name='TestDevice',
                                firmware_version='Version1',
                                serial_number='12345')
    sdcDevice = SdcDevice(wsdiscovery_instance,
                          my_uuid,
                          dpwsModel,
                          dpwsDevice,
                          my_mdib,
                          roleProvider=role_provider)
    for desc in sdcDevice.mdib.descriptions.objects:
        desc.SafetyClassification = pmtypes.SafetyClassification.MED_A
    sdcDevice.start_all(start_rtsample_loop=False)
    validators = [pmtypes.InstanceIdentifier('Validator', extension_string='System')]
    sdcDevice.set_location(location, validators)
    return sdcDevice


class Test_Tutorial(unittest.TestCase):
    """ run tutorial examples as unit tests, so that broken examples are automatically detected"""

    def setUp(self) -> None:
        self.my_location = SdcLocation(fac='ODDS',
                                       poc='CU1',
                                       bed='BedSim')
        self.my_location2 = SdcLocation(fac='ODDS',
                                        poc='CU2',
                                        bed='BedSim')
        # tests fill these lists with what they create, teardown cleans up after them.
        self.my_devices = []
        self.my_clients = []
        self.my_wsdiscoveries = []

    def tearDown(self) -> None:
        for cl in self.my_clients:
            print('stopping {}'.format(cl))
            cl.stop_all()
        for d in self.my_devices:
            print('stopping {}'.format(d))
            d.stop_all()
        for w in self.my_wsdiscoveries:
            print('stopping {}'.format(w))
            w.stop()

    def test_createDevice(self):
        # A WsDiscovery instance is needed to publish devices on the network.
        # In this case we want to publish them only on localhost 127.0.0.1.
        my_wsDiscovery = WSDiscoveryWhitelist(['127.0.0.1'])
        self.my_wsdiscoveries.append(my_wsDiscovery)
        my_wsDiscovery.start()

        # to create a device, this what you usually do:
        my_genericDevice = createGenericDevice(my_wsDiscovery, self.my_location, my_mdib_path)
        self.my_devices.append(my_genericDevice)

    def test_searchDevice(self):
        # create one discovery and two device that we can then search for
        my_wsDiscovery = WSDiscoveryWhitelist(['127.0.0.1'])
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
        my_client_wsDiscovery = WSDiscoverySingleAdapter(loopback_adapter)
        self.my_wsdiscoveries.append(my_client_wsDiscovery)
        my_client_wsDiscovery.start()

        # there a different methods to detect devices:
        # without specifying a type and a location, every WsDiscovery compatible device will be detected
        # (that can even be printers).
        services = my_client_wsDiscovery.search_services(timeout=SEARCH_TIMEOUT)
        self.assertEqual(len(services), 2)  # both devices found

        # now search only for devices in my_location2
        services = my_client_wsDiscovery.search_services(scopes=[Scope(self.my_location2.scope_string_sdc)],
                                                        timeout=SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)

        # search for medical devices only (BICEPS FInal version only)
        services = my_client_wsDiscovery.search_services(types=SDC_v1_Definitions.MedicalDeviceTypesFilter,
                                                        timeout=SEARCH_TIMEOUT)
        self.assertEqual(len(services), 2)

        # search for medical devices only all known protocol versions
        all_types = [p.MedicalDeviceTypesFilter for p in ProtocolsRegistry.protocols]
        services = my_client_wsDiscovery.search_multiple_types(types_list=all_types,
                                                             timeout=SEARCH_TIMEOUT)

        self.assertEqual(len(services), 2)

    def test_createClient(self):
        # create one discovery and one device that we can then search for
        my_wsDiscovery = WSDiscoveryWhitelist(['127.0.0.1'])
        self.my_wsdiscoveries.append(my_wsDiscovery)
        my_wsDiscovery.start()

        my_genericDevice1 = createGenericDevice(my_wsDiscovery, self.my_location, my_mdib_path)
        self.my_devices.append(my_genericDevice1)

        my_client_wsDiscovery = WSDiscoveryWhitelist(['127.0.0.1'])
        self.my_wsdiscoveries.append(my_client_wsDiscovery)
        my_client_wsDiscovery.start()

        # there a different methods to detect devices:
        # without specifying a type and a location, every WsDiscovery compatible device will be detected
        # (that can even be printers).
        services = my_client_wsDiscovery.search_services(timeout=SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)  # both devices found

        my_client = SdcClient.from_wsd_service(services[0])
        self.my_clients.append(my_client)
        my_client.start_all()
        ############# Mdib usage ##############################
        # In data oriented tests a mdib instance is very handy:
        # The mdib collects all data and makes it easily available for the test
        # The MdibContainer wraps data in "container" objects.
        # The basic idea is that every node that has a handle becomes directly accessible via its handle.
        myMdib = ClientMdibContainer(my_client)
        myMdib.init_mdib()  # myMdib keeps itself now updated

        # now query some data
        # mdib has three lookups: descriptions, states and context_states
        # each lookup can be searched by different keeys,
        # e.g looking for a descriptor by type looks like this:
        locationContextDescriptorContainers = myMdib.descriptions.NODETYPE.get(domTag('LocationContextDescriptor'))
        self.assertEqual(len(locationContextDescriptorContainers), 1)
        # we can look for the corresponding state by handle:
        locationContextStateContainers = myMdib.context_states.descriptorHandle.get(
            locationContextDescriptorContainers[0].handle)
        self.assertEqual(len(locationContextStateContainers), 1)

    def test_callOperation(self):
        # create one discovery and one device that we can then search for
        my_wsDiscovery = WSDiscoveryWhitelist(['127.0.0.1'])
        self.my_wsdiscoveries.append(my_wsDiscovery)
        my_wsDiscovery.start()

        my_genericDevice1 = createGenericDevice(my_wsDiscovery, self.my_location, my_mdib_path)
        self.my_devices.append(my_genericDevice1)

        my_client_wsDiscovery = WSDiscoveryWhitelist(['127.0.0.1'])
        self.my_wsdiscoveries.append(my_client_wsDiscovery)
        my_client_wsDiscovery.start()

        # there a different methods to detect devices:
        # without specifying a type and a location, every WsDiscovery compatible device will be detected
        # (that can even be printers).
        services = my_client_wsDiscovery.search_services(timeout=SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)  # both devices found

        my_client = SdcClient.from_wsd_service(services[0])
        self.my_clients.append(my_client)
        my_client.start_all()
        myMdib = ClientMdibContainer(my_client)
        myMdib.init_mdib()

        # we want to set a patient.
        # first we must find the operation that has PatientContextDescriptor as operation target
        patientContextDescriptorContainers = myMdib.descriptions.NODETYPE.get(domTag('PatientContextDescriptor'))
        self.assertEqual(len(patientContextDescriptorContainers), 1)
        myPatientContextDescriptorContainer = patientContextDescriptorContainers[0]
        all_operations = myMdib.descriptions.NODETYPE.get(domTag('SetContextStateOperationDescriptor'), [])
        my_operations = [op for op in all_operations if
                         op.OperationTarget == myPatientContextDescriptorContainer.handle]
        self.assertEqual(len(my_operations), 1)
        my_operation = my_operations[0]

        # make a proposed patient context:
        contextService = my_client.context_service_client
        proposedPatient = contextService.mk_proposed_context_object(myPatientContextDescriptorContainer.handle)
        proposedPatient.Firstname = 'Jack'
        proposedPatient.Lastname = 'Miller'
        future = contextService.set_context_state(operation_handle=my_operation.handle,
                                                proposed_context_states=[proposedPatient])
        result = future.result(timeout=5)
        self.assertEqual(result.state, pmtypes.InvocationState.FINISHED)

    def test_operation_handler(self):
        """ This example shows how to implement own handlers for operations and it shows multiple ways how a client can
        find the desired operation.
        """
        # these codes of the mdib are used in this test:
        MY_CODE_1 = CodedValue('196279')  # refers to an activate operation in mdib
        MY_CODE_2 = CodedValue('196278')  # refers to a set string operation
        MY_CODE_3 = CodedValue('196276')  # refers to a set value operations
        MY_CODE_3_TARGET = CodedValue('196274')  # this is the operation target for MY_CODE_3

        class MyProvider1(ProviderRole):
            """ This provider handles operations with code == MY_CODE_1 and MY_CODE_2.
            Operations with these codes already exist in the mdib that is used for this test. """

            def __init__(self, log_prefix):
                super().__init__(log_prefix)
                self.operation1_called = 0
                self.operation1_args = None
                self.operation2_called = 0
                self.operation2_args = None

            def make_operation_instance(self, operation_descriptor_container, operations_factory):
                """ if the role provider is responsible for handling of calls to this operation_descriptor_container,
                 it creates an operation instance and returns it. Otherwise it returns None"""
                if operation_descriptor_container.coding == MY_CODE_1.coding:
                    # This is a very simple check that only checks the code of the operation.
                    # Depending on your use case, you could also check the operation target is the correct one,
                    # or if this is a child of a specific VMD, ...
                    #
                    # The following line shows how to provide your callback (in this case self._handle_operation_1).
                    # This callback is called when a consumer calls the operation.
                    operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                         operations_factory,
                                                                         current_argument_handler=self._handle_operation_1)
                    return operation
                elif operation_descriptor_container.coding == MY_CODE_2.coding:
                    operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                         operations_factory,
                                                                         current_argument_handler=self._handle_operation_2)
                    return operation
                else:
                    return None

            def _handle_operation_1(self, operation_instance, argument):
                """This operation does not manipulate the mdib at all, it only registers the call."""
                self.operation1_called += 1
                self.operation1_args = argument
                self._logger.info('_handle_operation_1 called')

            def _handle_operation_2(self, operation_instance, argument):
                """This operation manipulate it operation target, and only registers the call."""
                self.operation2_called += 1
                self.operation2_args = argument
                self._logger.info('_handle_operation_2 called')
                with self._mdib.transaction_manager() as mgr:
                    my_state = mgr.get_state(operation_instance.operation_target_handle)
                    if my_state.metricValue is None:
                        my_state.mk_metric_value()
                    my_state.metricValue.Value = argument

        class MyProvider2(ProviderRole):
            """ This provider handles operations with code == MY_CODE_3.
            Operations with these codes already exist in the mdib that is used for this test. """

            def __init__(self, log_prefix):
                super().__init__(log_prefix)
                self.operation3_args = None
                self.operation3_called = 0

            def make_operation_instance(self, operation_descriptor_container, operations_factory):
                if operation_descriptor_container.coding == MY_CODE_3.coding:
                    self._logger.info(
                        'instantiating operation 3 from existing descriptor handle={}'.format(
                            operation_descriptor_container.handle))
                    operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                         operations_factory,
                                                                         current_argument_handler=self._handle_operation_3)
                    return operation
                else:
                    return None

            def _handle_operation_3(self, operation_instance, argument):
                """This operation manipulate it operation target, and only registers the call."""
                self.operation3_called += 1
                self.operation3_args = argument
                self._logger.info('_handle_operation_3 called')
                with self._mdib.transaction_manager() as mgr:
                    my_state = mgr.get_state(operation_instance.operation_target_handle)
                    if my_state.metricValue is None:
                        my_state.mk_metric_value()
                    my_state.metricValue.Value = argument

        class MyProductImpl(BaseProduct):
            """This class provides all handlers of the fictional product.
            It instantiates 2 role providers.
            The number of role providers does not matter, it is a question of how the code is organized.
            Each role provider should handle one specific role, e.g audio pause provider, clock provider, ..."""

            def __init__(self, log_prefix=None):
                super().__init__(log_prefix)
                self.my_provider_1 = MyProvider1(log_prefix=log_prefix)
                self._ordered_providers.append(self.my_provider_1)
                self.my_provider_2 = MyProvider2(log_prefix=log_prefix)
                self._ordered_providers.append(self.my_provider_2)

        # Create a device like in the examples above, but provide an own role provider.
        # This role provider is used instead of the default one.
        my_wsDiscovery = WSDiscoveryWhitelist(['127.0.0.1'])
        self.my_wsdiscoveries.append(my_wsDiscovery)
        my_wsDiscovery.start()

        my_product_impl = MyProductImpl(log_prefix='p1')

        # use the minimalistic mdib from reference test:
        _here = os.path.dirname(__file__)
        mdib_path = os.path.join(_here, '../examples/ReferenceTest/reference_mdib.xml')
        my_genericDevice = createGenericDevice(my_wsDiscovery,
                                               self.my_location,
                                               mdib_path,
                                               role_provider=my_product_impl)

        self.my_devices.append(my_genericDevice)

        # connect a client to this device:
        my_client_wsDiscovery = WSDiscoveryWhitelist(['127.0.0.1'])
        self.my_wsdiscoveries.append(my_client_wsDiscovery)
        my_client_wsDiscovery.start()

        services = my_client_wsDiscovery.search_services(timeout=SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)

        my_client = SdcClient.from_wsd_service(services[0])
        self.my_clients.append(my_client)
        my_client.start_all()
        myMdib = ClientMdibContainer(my_client)
        myMdib.init_mdib()

        # call activate operation:
        # As a client NEVER! use the handle of the operation directly, always use the code(s) to identify things.
        # Handles are random values without any meaning, they are only unique id's in the mdib.
        operations = myMdib.descriptions.coding.get(MY_CODE_1.coding)
        # the mdib contains 2 operations with the same code. To keep things simple, just use the first one here.
        op = operations[0]
        future = my_client.set_service_client.activate(op.handle, 'foo')
        result = future.result()
        print(result)
        self.assertEqual(my_product_impl.my_provider_1.operation1_called, 1)
        # There is a inconsistency in the current implementation of activate:
        # in general activate allows multiple arguments, but the current implementation only allows a single
        # one. Therefore we provide a single string, but expect a list with the single string.
        # This inconsistency will be fixed in a later version of sdc11073
        self.assertEqual(my_product_impl.my_provider_1.operation1_args, ['foo'])

        # call set_string operation
        op = myMdib.descriptions.coding.getOne(MY_CODE_2.coding)
        for value in ('foo', 'bar'):
            future = my_client.set_service_client.set_string(op.handle, value)
            result = future.result()
            print(result)
            self.assertEqual(my_product_impl.my_provider_1.operation2_args, value)
            state = myMdib.states.descriptorHandle.getOne(op.OperationTarget)
            self.assertEqual(state.metricValue.Value, value)
        self.assertEqual(my_product_impl.my_provider_1.operation2_called, 2)

        # call setValue operation
        state_descr = myMdib.descriptions.coding.getOne(MY_CODE_3_TARGET.coding)
        operations = myMdib.get_operation_descriptors_for_descriptor_handle(state_descr.Handle)
        op = operations[0]
        future = my_client.set_service_client.set_numeric_value(op.handle, 42)
        result = future.result()
        print(result)
        self.assertEqual(my_product_impl.my_provider_2.operation3_args, 42)
        state = myMdib.states.descriptorHandle.getOne(op.OperationTarget)
        self.assertEqual(state.metricValue.Value, 42)
