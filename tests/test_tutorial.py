from __future__ import annotations

import os
import unittest
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING

from sdc11073 import network
from sdc11073.consumer import SdcConsumer
from sdc11073.definitions_base import ProtocolsRegistry
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.loghelper import basic_logging_setup, get_logger_adapter
from sdc11073.mdib import ProviderMdib
from sdc11073.mdib.consumermdib import ConsumerMdib
from sdc11073.provider import SdcProvider
from sdc11073.provider.components import SdcProviderComponents
from sdc11073.provider.operations import ExecuteResult
from sdc11073.roles.product import BaseProduct
from sdc11073.roles.providerbase import ProviderRole
from sdc11073.wsdiscovery import WSDiscovery, WSDiscoverySingleAdapter
from sdc11073.xml_types import msg_types, pm_types
from sdc11073.xml_types import pm_qnames as pm
from sdc11073.xml_types.dpws_types import ThisDeviceType, ThisModelType
from sdc11073.xml_types.msg_types import InvocationState
from sdc11073.xml_types.pm_types import CodedValue
from sdc11073.xml_types.wsd_types import ScopesType
from sdc11073.xml_types.actions import periodic_actions_and_system_error_report
from tests import utils

if TYPE_CHECKING:
    from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
    from sdc11073.provider.operations import ExecuteParameters, OperationDefinitionBase
    from sdc11073.roles.providerbase import OperationClassGetter

loopback_adapter = next(adapter for adapter in network.get_adapters() if adapter.is_loopback)

SEARCH_TIMEOUT = 2  # in real world applications this timeout is too short, 10 seconds is a good value.
# Here this short timeout is used to accelerate the test.

here = os.path.dirname(__file__)
my_mdib_path = os.path.join(here, '70041_MDIB_Final.xml')


def createGenericDevice(wsdiscovery_instance, location, mdib_path, specific_components=None):
    my_mdib = ProviderMdib.from_mdib_file(mdib_path)
    my_epr = uuid.uuid4().hex
    this_model = ThisModelType(manufacturer='Draeger',
                               manufacturer_url='www.draeger.com',
                               model_name='TestDevice',
                               model_number='1.0',
                               model_url='www.draeger.com/model',
                               presentation_url='www.draeger.com/model/presentation')

    this_device = ThisDeviceType(friendly_name='TestDevice',
                                 firmware_version='Version1',
                                 serial_number='12345')
    sdc_device = SdcProvider(wsdiscovery_instance,
                             this_model,
                             this_device,
                             my_mdib,
                             epr=my_epr,
                             specific_components=specific_components)
    for desc in sdc_device.mdib.descriptions.objects:
        desc.SafetyClassification = pm_types.SafetyClassification.MED_A
    sdc_device.start_all(start_rtsample_loop=False)
    validators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
    sdc_device.set_location(location, validators)
    return sdc_device


MY_CODE_1 = CodedValue('196279')  # refers to an activate operation in mdib
MY_CODE_2 = CodedValue('196278')  # refers to a set string operation
MY_CODE_3 = CodedValue('196276')  # refers to a set value operations
MY_CODE_3_TARGET = CodedValue('196274')  # this is the operation target for MY_CODE_3


class MyProvider1(ProviderRole):
    """This provider handles operations with code == MY_CODE_1 and MY_CODE_2.

    Operations with these codes already exist in the mdib that is used for this test.
    """

    def __init__(self, mdib, log_prefix):
        super().__init__(mdib, log_prefix)
        self.operation1_called = 0
        self.operation1_args = None
        self.operation2_called = 0
        self.operation2_args = None

    def make_operation_instance(self,
                                operation_descriptor_container: AbstractOperationDescriptorProtocol,
                                operation_cls_getter: OperationClassGetter) -> OperationDefinitionBase | None:
        """If the role provider is responsible for handling of calls to this operation_descriptor_container,
        it creates an operation instance and returns it, otherwise it returns None.
        """
        if operation_descriptor_container.coding == MY_CODE_1.coding:
            # This is a very simple check that only checks the code of the operation.
            # Depending on your use case, you could also check the operation target is the correct one,
            # or if this is a child of a specific VMD, ...
            #
            # The following line shows how to provide your callback (in this case self._handle_operation_1).
            # This callback is called when a consumer calls the operation.
            operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                     operation_cls_getter,
                                                                     self._handle_operation_1)
            return operation
        if operation_descriptor_container.coding == MY_CODE_2.coding:
            operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                     operation_cls_getter,
                                                                     self._handle_operation_2)
            return operation
        return None

    def _handle_operation_1(self, params: ExecuteParameters) -> ExecuteResult:
        """This operation does not manipulate the mdib at all, it only registers the call."""
        argument = params.operation_request.argument
        self.operation1_called += 1
        self.operation1_args = argument
        self._logger.info('_handle_operation_1 called arg={}', argument)
        return ExecuteResult(params.operation_instance.operation_target_handle, InvocationState.FINISHED)

    def _handle_operation_2(self, params: ExecuteParameters) -> ExecuteResult:
        """This operation manipulate it operation target, and only registers the call."""
        argument = params.operation_request.argument
        self.operation2_called += 1
        self.operation2_args = argument
        self._logger.info('_handle_operation_2 called arg={}', argument)
        with self._mdib.transaction_manager() as mgr:
            my_state = mgr.get_state(params.operation_instance.operation_target_handle)
            if my_state.MetricValue is None:
                my_state.mk_metric_value()
            my_state.MetricValue.Value = argument
        return ExecuteResult(params.operation_instance.operation_target_handle, InvocationState.FINISHED)


class MyProvider2(ProviderRole):
    """This provider handles operations with code == MY_CODE_3.
    Operations with these codes already exist in the mdib that is used for this test.
    """

    def __init__(self, mdib, log_prefix):
        super().__init__(mdib, log_prefix)
        self.operation3_args = None
        self.operation3_called = 0

    def make_operation_instance(self,
                                operation_descriptor_container: AbstractOperationDescriptorProtocol,
                                operation_cls_getter: OperationClassGetter) -> OperationDefinitionBase | None:

        if operation_descriptor_container.coding == MY_CODE_3.coding:
            self._logger.info(
                'instantiating operation 3 from existing descriptor handle={}'.format(
                    operation_descriptor_container.Handle))
            operation = self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                     operation_cls_getter,
                                                                     self._handle_operation_3)
            return operation
        else:
            return None

    def _handle_operation_3(self, params: ExecuteParameters) -> ExecuteResult:
        """This operation manipulate it operation target, and only registers the call."""
        self.operation3_called += 1
        argument = params.operation_request.argument
        self.operation3_args = argument
        self._logger.info('_handle_operation_3 called')
        with self._mdib.transaction_manager() as mgr:
            my_state = mgr.get_state(params.operation_instance.operation_target_handle)
            if my_state.MetricValue is None:
                my_state.mk_metric_value()
            my_state.MetricValue.Value = argument
        return ExecuteResult(params.operation_instance.operation_target_handle, InvocationState.FINISHED)


class MyProductImpl(BaseProduct):
    """This class provides all handlers of the fictional product.
    It instantiates 2 role providers.
    The number of role providers does not matter, it is a question of how the code is organized.
    Each role provider should handle one specific role, e.g. audio pause provider, clock provider, ...
    """

    def __init__(self, mdib, sco, log_prefix=None):
        super().__init__(mdib, sco, log_prefix)
        self.my_provider_1 = MyProvider1(mdib, log_prefix=log_prefix)
        self._ordered_providers.append(self.my_provider_1)
        self.my_provider_2 = MyProvider2(mdib, log_prefix=log_prefix)
        self._ordered_providers.append(self.my_provider_2)


class Test_Tutorial(unittest.TestCase):
    """run tutorial examples as unit tests, so that broken examples are automatically detected."""

    def setUp(self) -> None:
        self.my_location = utils.random_location()
        self.my_location2 = utils.random_location()
        # tests fill these lists with what they create, teardown cleans up after them.
        self.my_devices = []
        self.my_clients = []
        self.my_ws_discoveries = []

        basic_logging_setup()
        self._logger = get_logger_adapter('sdc.tutorial')
        self._logger.info('###### setUp done ##########')

    def tearDown(self) -> None:
        self._logger.info('###### tearDown ... ##########')
        for cl in self.my_clients:
            self._logger.info('stopping {}', cl)
            cl.stop_all()
        for d in self.my_devices:
            self._logger.info('stopping {}', d)
            d.stop_all()
        for w in self.my_ws_discoveries:
            self._logger.info('stopping {}', w)
            w.stop()

    def test_createDevice(self):
        # A WsDiscovery instance is needed to publish devices on the network.
        # In this case we want to publish them only on localhost 127.0.0.1.
        my_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_ws_discovery)
        my_ws_discovery.start()

        # to create a device, this what you usually do:
        my_generic_device = createGenericDevice(my_ws_discovery, self.my_location, my_mdib_path)
        self.my_devices.append(my_generic_device)

    def test_searchDevice(self):
        # create one discovery and two device that we can then search for
        my_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_ws_discovery)
        my_ws_discovery.start()

        my_generic_device1 = createGenericDevice(my_ws_discovery, self.my_location, my_mdib_path)
        self.my_devices.append(my_generic_device1)

        my_generic_device2 = createGenericDevice(my_ws_discovery, self.my_location2, my_mdib_path)
        self.my_devices.append(my_generic_device2)

        # Search for devices
        # ------------------
        # create a new discovery instance for searching.
        # (technically this would not be necessary, but it makes things much clearer in our example)
        # for searching we use again localhost adapter. For demonstration purpose a WSDiscoverySingleAdapter is used
        my_client_ws_discovery = WSDiscoverySingleAdapter(loopback_adapter.name)
        self.my_ws_discoveries.append(my_client_ws_discovery)
        my_client_ws_discovery.start()

        # there a different methods to detect devices:
        # without specifying a type and a location, every WsDiscovery compatible device will be detected
        # (that can even be printers).
        # TODO: enable this step once https://github.com/Draegerwerk/sdc11073/issues/223 has been fixed

        # now search only for devices in my_location2
        services = my_client_ws_discovery.search_services(scopes=ScopesType(self.my_location2.scope_string),
                                                          timeout=SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)

        # search for medical devices only (BICEPS Final version only)
        services = my_client_ws_discovery.search_services(types=SdcV1Definitions.MedicalDeviceTypesFilter,
                                                          timeout=SEARCH_TIMEOUT)
        self.assertGreaterEqual(len(services), 2)

        # search for medical devices only all known protocol versions
        all_types = [p.MedicalDeviceTypesFilter for p in ProtocolsRegistry.protocols]
        services = my_client_ws_discovery.search_multiple_types(types_list=all_types,
                                                                timeout=SEARCH_TIMEOUT)

        self.assertGreaterEqual(len(services), 2)

    def test_createClient(self):
        # create one discovery and one device that we can then search for
        my_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_ws_discovery)
        my_ws_discovery.start()

        my_generic_device1 = createGenericDevice(my_ws_discovery, self.my_location, my_mdib_path)
        self.my_devices.append(my_generic_device1)

        my_client_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_client_ws_discovery)
        my_client_ws_discovery.start()

        # there a different methods to detect devices:
        # without specifying a type and a location, every WsDiscovery compatible device will be detected
        # (that can even be printers).
        services = my_client_ws_discovery.search_services(timeout=SEARCH_TIMEOUT,
                                                          scopes=ScopesType(self.my_location.scope_string))
        self.assertEqual(len(services), 1)  # both devices found

        my_client = SdcConsumer.from_wsd_service(services[0], ssl_context_container=None)
        self.my_clients.append(my_client)
        my_client.start_all(not_subscribed_actions=periodic_actions_and_system_error_report)
        ############# Mdib usage ##############################
        # In data oriented tests a mdib instance is very handy:
        # The mdib collects all data and makes it easily available for the test
        # The MdibBase wraps data in "container" objects.
        # The basic idea is that every node that has a handle becomes directly accessible via its handle.
        my_mdib = ConsumerMdib(my_client)
        my_mdib.init_mdib()  # my_mdib keeps itself now updated

        # now query some data
        # mdib has three lookups: descriptions, states and context_states
        # each lookup can be searched by different keys,
        # e.g. looking for a descriptor by type looks like this:
        location_context_descriptor_containers = my_mdib.descriptions.NODETYPE.get(pm.LocationContextDescriptor)
        self.assertEqual(len(location_context_descriptor_containers), 1)
        # we can look for the corresponding state by handle:
        location_context_state_containers = my_mdib.context_states.descriptor_handle.get(
            location_context_descriptor_containers[0].Handle)
        self.assertEqual(len(location_context_state_containers), 1)

    def test_call_operation(self):
        # create one discovery and one device that we can then search for
        my_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_ws_discovery)
        my_ws_discovery.start()

        my_generic_device1 = createGenericDevice(my_ws_discovery, self.my_location, my_mdib_path)
        self.my_devices.append(my_generic_device1)

        my_client_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_client_ws_discovery)
        my_client_ws_discovery.start()

        # there a different methods to detect devices:
        # without specifying a type and a location, every WsDiscovery compatible device will be detected
        # (that can even be printers).
        services = my_client_ws_discovery.search_services(timeout=SEARCH_TIMEOUT,
                                                          scopes=ScopesType(self.my_location.scope_string))
        self.assertEqual(len(services), 1)  # both devices found

        my_client = SdcConsumer.from_wsd_service(services[0], ssl_context_container=None)
        self.my_clients.append(my_client)
        my_client.start_all(not_subscribed_actions=periodic_actions_and_system_error_report)
        my_mdib = ConsumerMdib(my_client)
        my_mdib.init_mdib()

        # we want to set a patient.
        # first we must find the operation that has PatientContextDescriptor as operation target
        patient_context_descriptor_containers = my_mdib.descriptions.NODETYPE.get(pm.PatientContextDescriptor)
        self.assertEqual(len(patient_context_descriptor_containers), 1)
        my_patient_context_descriptor_container = patient_context_descriptor_containers[0]
        all_operations = my_mdib.descriptions.NODETYPE.get(pm.SetContextStateOperationDescriptor, [])
        my_operations = [op for op in all_operations if
                         op.OperationTarget == my_patient_context_descriptor_container.Handle]
        self.assertEqual(len(my_operations), 1)
        my_operation = my_operations[0]

        # make a proposed patient context:
        context_service = my_client.context_service_client
        proposed_patient = context_service.mk_proposed_context_object(my_patient_context_descriptor_container.Handle)
        proposed_patient.Firstname = 'Jack'
        proposed_patient.Lastname = 'Miller'
        future = context_service.set_context_state(operation_handle=my_operation.Handle,
                                                   proposed_context_states=[proposed_patient])
        result = future.result(timeout=5)
        self.assertEqual(result.InvocationInfo.InvocationState, msg_types.InvocationState.FINISHED)

    def test_operation_handler(self):
        """This example shows how to implement own handlers for operations, and it shows multiple ways how a client can
        find the desired operation.
        """
        # Create a device like in the examples above, but provide an own role provider.
        # This role provider is used instead of the default one.
        my_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_ws_discovery)
        my_ws_discovery.start()

        specific_components = SdcProviderComponents(role_provider_class=MyProductImpl)
        # use the minimalistic mdib from reference test:
        mdib_path = os.path.join(here, '../examples/ReferenceTest/reference_mdib.xml')
        my_generic_device = createGenericDevice(my_ws_discovery,
                                                self.my_location,
                                                mdib_path,
                                                specific_components=specific_components)

        self.my_devices.append(my_generic_device)

        # connect a client to this device:
        my_client_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_client_ws_discovery)
        my_client_ws_discovery.start()

        services = my_client_ws_discovery.search_services(timeout=SEARCH_TIMEOUT,
                                                          scopes=ScopesType(self.my_location.scope_string))
        self.assertEqual(len(services), 1)

        self.service = SdcConsumer.from_wsd_service(services[0], ssl_context_container=None)
        my_client = self.service
        self.my_clients.append(my_client)
        my_client.start_all(not_subscribed_actions=periodic_actions_and_system_error_report)
        my_mdib = ConsumerMdib(my_client)
        my_mdib.init_mdib()

        sco_handle = 'sco.mds0'
        my_product_impl = my_generic_device.product_lookup[sco_handle]
        # call activate operation:
        # A client should NEVER! use the handle of the operation directly, always use the code(s) to identify things.
        # Handles are random values without any meaning, they are only unique id's in the mdib.
        operations = my_mdib.descriptions.coding.get(MY_CODE_1.coding)
        # the mdib contains 2 operations with the same code. To keep things simple, just use the first one here.
        self._logger.info('looking for operations with code {}', MY_CODE_1.coding)
        op = operations[0]
        argument = 'foo'
        self._logger.info('calling operation {}, argument = {}', op, argument)
        future = my_client.set_service_client.activate(op.Handle, arguments=[argument])
        result = future.result()
        print(result)
        self.assertEqual(my_product_impl.my_provider_1.operation1_called, 1)
        args = my_product_impl.my_provider_1.operation1_args
        self.assertEqual(1, len(args))
        self.assertEqual(args[0].ArgValue, 'foo')

        # call set_string operation
        sco_handle = 'sco.vmd1.mds0'
        my_product_impl = my_generic_device.product_lookup[sco_handle]

        self._logger.info('looking for operations with code {}', MY_CODE_2.coding)
        op = my_mdib.descriptions.coding.get_one(MY_CODE_2.coding)
        for value in ('foo', 'bar'):
            self._logger.info('calling operation {}, argument = {}', op, value)
            future = my_client.set_service_client.set_string(op.Handle, value)
            result = future.result()
            print(result)
            self.assertEqual(my_product_impl.my_provider_1.operation2_args, value)
            state = my_mdib.states.descriptor_handle.get_one(op.OperationTarget)
            self.assertEqual(state.MetricValue.Value, value)
        self.assertEqual(my_product_impl.my_provider_1.operation2_called, 2)

        # call setValue operation
        state_descr = my_mdib.descriptions.coding.get_one(MY_CODE_3_TARGET.coding)
        operations = my_mdib.get_operation_descriptors_for_descriptor_handle(state_descr.Handle)
        op = operations[0]
        future = my_client.set_service_client.set_numeric_value(op.Handle, Decimal('42'))
        result = future.result()
        print(result)
        self.assertEqual(my_product_impl.my_provider_2.operation3_args, 42)
        state = my_mdib.states.descriptor_handle.get_one(op.OperationTarget)
        self.assertEqual(state.MetricValue.Value, 42)
