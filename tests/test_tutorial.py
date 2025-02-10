"""The module contains example how to use sdc provider and consumer."""
from __future__ import annotations

import os
import time
import unittest
import uuid
from decimal import Decimal
from typing import TYPE_CHECKING

from sdc11073 import network
from sdc11073.consumer import SdcConsumer
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.entity_mdib.entity_consumermdib import EntityConsumerMdib
from sdc11073.entity_mdib.entity_providermdib import EntityProviderMdib
from sdc11073.loghelper import basic_logging_setup, get_logger_adapter
from sdc11073.provider import SdcProvider
from sdc11073.provider.components import SdcProviderComponents
from sdc11073.provider.operations import ExecuteResult
from sdc11073.roles.product import BaseProduct
from sdc11073.roles.providerbase import ProviderRole
from sdc11073.wsdiscovery import WSDiscovery, WSDiscoverySingleAdapter
from sdc11073.xml_types import msg_types, pm_types
from sdc11073.xml_types import pm_qnames as pm
from sdc11073.xml_types.actions import periodic_actions_and_system_error_report
from sdc11073.xml_types.dpws_types import ThisDeviceType, ThisModelType
from sdc11073.xml_types.msg_types import InvocationState
from sdc11073.xml_types.pm_types import CodedValue
from sdc11073.xml_types.wsd_types import ScopesType
from tests import utils

if TYPE_CHECKING:
    from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
    from sdc11073.mdib.mdibprotocol import ProviderMdibProtocol
    from sdc11073.provider.operations import ExecuteParameters, OperationDefinitionBase
    from sdc11073.provider.sco import AbstractScoOperationsRegistry
    from sdc11073.roles.providerbase import OperationClassGetter

loopback_adapter = next(adapter for adapter in network.get_adapters() if adapter.is_loopback)

SEARCH_TIMEOUT = 2  # in real world applications this timeout is too short, 10 seconds is a good value.
# Here this short timeout is used to accelerate the test.

here = os.path.dirname(__file__)  # noqa: PTH120
my_mdib_path = os.path.join(here, '70041_MDIB_Final.xml')  # noqa: PTH118


def create_generic_provider(wsdiscovery_instance: WSDiscovery,
                            location: str,
                            mdib_path: str,
                            specific_components: SdcProviderComponents | None = None) -> SdcProvider:
    my_mdib = EntityProviderMdib.from_mdib_file(mdib_path)
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
    sdc_provider = SdcProvider(wsdiscovery_instance,
                               this_model,
                               this_device,
                               my_mdib,
                               epr=my_epr,
                               specific_components=specific_components)
    with sdc_provider.mdib.descriptor_transaction() as tr:
        for _, ent in sdc_provider.mdib.entities.items():  # noqa: PERF102
            ent.descriptor.SafetyClassification = pm_types.SafetyClassification.MED_A
            tr.write_entity(ent)
    sdc_provider.start_all(start_rtsample_loop=False)
    validators = [pm_types.InstanceIdentifier('Validator', extension_string='System')]
    sdc_provider.set_location(location, validators)
    return sdc_provider


MY_CODE_1 = CodedValue('196279')  # refers to an activate operation in mdib
MY_CODE_2 = CodedValue('196278')  # refers to a set string operation
MY_CODE_3 = CodedValue('196276')  # refers to a set value operations
MY_CODE_3_TARGET = CodedValue('196274')  # this is the operation target for MY_CODE_3


class MyProvider1(ProviderRole):
    """The provider handles operations with code == MY_CODE_1 and MY_CODE_2.

    Operations with these codes already exist in the mdib that is used for this test.
    """

    def __init__(self,
                 mdib: ProviderMdibProtocol,
                 log_prefix: str):
        super().__init__(mdib, log_prefix)
        self.operation1_called = 0
        self.operation1_args = None
        self.operation2_called = 0
        self.operation2_args = None

    def make_operation_instance(self,
                                operation_descriptor_container: AbstractOperationDescriptorProtocol,
                                operation_cls_getter: OperationClassGetter) -> OperationDefinitionBase | None:
        """Create an operation instance if operation_descriptor_container matches this operation.

        If the role provider is responsible for handling of calls to this operation_descriptor_container,
        it creates an operation instance and returns it, otherwise it returns None.
        """
        if operation_descriptor_container.coding == MY_CODE_1.coding:
            # This is a very simple check that only checks the code of the operation.
            # Depending on your use case, you could also check the operation target is the correct one,
            # or if this is a child of a specific VMD, ...
            #
            # The following line shows how to provide your callback (in this case self._handle_operation_1).
            # This callback is called when a consumer calls the operation.
            return self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                operation_cls_getter,
                                                                self._handle_operation_1)
        if operation_descriptor_container.coding == MY_CODE_2.coding:
            return self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                operation_cls_getter,
                                                                self._handle_operation_2)
        return None

    def _handle_operation_1(self, params: ExecuteParameters) -> ExecuteResult:
        """Do not manipulate the mdib at all, it only increment the call counter."""
        argument = params.operation_request.argument
        self.operation1_called += 1
        self.operation1_args = argument
        self._logger.info('_handle_operation_1 called arg=%r', argument)
        return ExecuteResult(params.operation_instance.operation_target_handle, InvocationState.FINISHED)

    def _handle_operation_2(self, params: ExecuteParameters) -> ExecuteResult:
        """Manipulate the operation target, and increments the call counter."""
        argument = params.operation_request.argument
        self.operation2_called += 1
        self.operation2_args = argument
        self._logger.info('_handle_operation_2 called arg=%r', argument)
        op_target_entity = self._mdib.entities.by_handle(params.operation_instance.operation_target_handle)
        if op_target_entity.state.MetricValue is None:
            op_target_entity.state.mk_metric_value()
        op_target_entity.state.MetricValue.Value = argument
        with self._mdib.metric_state_transaction() as mgr:
            mgr.write_entity(op_target_entity)
        return ExecuteResult(params.operation_instance.operation_target_handle, InvocationState.FINISHED)


class MyProvider2(ProviderRole):
    """The provider handles operations with code == MY_CODE_3.

    Operations with these codes already exist in the mdib that is used for this test.
    """

    def __init__(self, mdib: ProviderMdibProtocol, log_prefix: str):
        super().__init__(mdib, log_prefix)
        self.operation3_args = None
        self.operation3_called = 0

    def make_operation_instance(self,
                                operation_descriptor_container: AbstractOperationDescriptorProtocol,
                                operation_cls_getter: OperationClassGetter) -> OperationDefinitionBase | None:

        if operation_descriptor_container.coding == MY_CODE_3.coding:
            self._logger.info('instantiating operation 3 from existing descriptor handle=%s',
                              operation_descriptor_container.Handle)
            return self._mk_operation_from_operation_descriptor(operation_descriptor_container,
                                                                operation_cls_getter,
                                                                self._handle_operation_3)
        return None

    def _handle_operation_3(self, params: ExecuteParameters) -> ExecuteResult:
        """Manipulate the operation target, and increments the call counter."""
        self.operation3_called += 1
        argument = params.operation_request.argument
        self.operation3_args = argument
        self._logger.info('_handle_operation_3 called')
        op_target_entity = self._mdib.entities.by_handle(params.operation_instance.operation_target_handle)
        if op_target_entity.state.MetricValue is None:
            op_target_entity.state.mk_metric_value()
        op_target_entity.state.MetricValue.Value = argument
        with self._mdib.metric_state_transaction() as mgr:
            mgr.write_entity(op_target_entity)
        return ExecuteResult(params.operation_instance.operation_target_handle, InvocationState.FINISHED)


class MyProductImpl(BaseProduct):
    """The class provides all handlers of the fictional product.

    It instantiates 2 role providers.
    The number of role providers does not matter, it is a question of how the code is organized.
    Each role provider should handle one specific role, e.g. audio pause provider, clock provider, ...
    """

    def __init__(self,
                 mdib: ProviderMdibProtocol,
                 sco: AbstractScoOperationsRegistry,
                 log_prefix: str | None = None):
        super().__init__(mdib, sco, log_prefix)
        self.my_provider_1 = MyProvider1(mdib, log_prefix=log_prefix)
        self._ordered_providers.append(self.my_provider_1)
        self.my_provider_2 = MyProvider2(mdib, log_prefix=log_prefix)
        self._ordered_providers.append(self.my_provider_2)


class TestTutorial(unittest.TestCase):
    """run tutorial examples as unit tests, so that broken examples are automatically detected."""

    def setUp(self) -> None:
        self.my_location = utils.random_location()
        self.my_location2 = utils.random_location()
        # tests fill these lists with what they create, teardown cleans up after them.
        self.my_providers = []
        self.my_consumers = []
        self.my_ws_discoveries = []

        basic_logging_setup()
        self._logger = get_logger_adapter('sdc.tutorial')
        self._logger.info('###### setUp done ##########')

    def tearDown(self) -> None:
        self._logger.info('###### tearDown ... ##########')
        for consumer in self.my_consumers:
            self._logger.info('stopping %r', consumer)
            consumer.stop_all()
        for provider in self.my_providers:
            self._logger.info('stopping %r', provider)
            provider.stop_all()
        for discovery in self.my_ws_discoveries:
            self._logger.info('stopping %r', discovery)
            discovery.stop()

    def test_create_provider(self):
        # A WsDiscovery instance is needed to publish devices on the network.
        # In this case we want to publish them only on localhost 127.0.0.1.
        my_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_ws_discovery)
        my_ws_discovery.start()

        # to create a device, this what you usually do:
        my_generic_provider = create_generic_provider(my_ws_discovery, self.my_location, my_mdib_path)
        self.my_providers.append(my_generic_provider)

    def test_search_provider(self):
        # create one discovery and two device that we can then search for
        my_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_ws_discovery)
        my_ws_discovery.start()

        my_generic_provider1 = create_generic_provider(my_ws_discovery, self.my_location, my_mdib_path)
        self.my_providers.append(my_generic_provider1)

        my_generic_provider2 = create_generic_provider(my_ws_discovery, self.my_location2, my_mdib_path)
        self.my_providers.append(my_generic_provider2)

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

        # search for any device at my_location2
        services = my_client_ws_discovery.search_services(scopes=ScopesType(self.my_location2.scope_string),
                                                          timeout=SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)

        # search for medical devices at any location
        services = my_client_ws_discovery.search_services(types=SdcV1Definitions.MedicalDeviceTypesFilter,
                                                          timeout=SEARCH_TIMEOUT)
        self.assertGreaterEqual(len(services), 2)

    def test_create_client(self):
        # create one discovery and one device that we can then search for
        my_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_ws_discovery)
        my_ws_discovery.start()

        my_generic_provider1 = create_generic_provider(my_ws_discovery, self.my_location, my_mdib_path)
        self.my_providers.append(my_generic_provider1)

        my_client_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_client_ws_discovery)
        my_client_ws_discovery.start()

        # there a different methods to detect devices:
        # without specifying a type and a location, every WsDiscovery compatible device will be detected
        # (that can even be printers).
        services = my_client_ws_discovery.search_services(timeout=SEARCH_TIMEOUT,
                                                          scopes=ScopesType(self.my_location.scope_string))
        self.assertEqual(len(services), 1)  # both devices found

        my_consumer = SdcConsumer.from_wsd_service(services[0], ssl_context_container=None)
        self.my_consumers.append(my_consumer)
        my_consumer.start_all(not_subscribed_actions=periodic_actions_and_system_error_report)
        ############# Mdib usage ##############################
        # In data oriented tests a mdib instance is very handy:
        # The mdib collects all data and makes it easily available for the test
        # The MdibBase wraps data in "container" objects.
        # The basic idea is that every node that has a handle becomes directly accessible via its handle.
        my_mdib = EntityConsumerMdib(my_consumer)
        my_mdib.init_mdib()  # my_mdib keeps itself now updated

        # now query some data
        # mdib has three lookups: descriptions, states and context_states
        # each lookup can be searched by different keys,
        # e.g. looking for a descriptor by type looks like this:
        location_context_entities = my_mdib.entities.by_node_type(pm.LocationContextDescriptor)
        self.assertEqual(len(location_context_entities), 1)
        self.assertEqual(len(location_context_entities[0].states), 1)

    def test_call_operation(self):
        # create one discovery and one device that we can then search for
        my_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_ws_discovery)
        my_ws_discovery.start()

        my_generic_provider1 = create_generic_provider(my_ws_discovery, self.my_location, my_mdib_path)
        self.my_providers.append(my_generic_provider1)

        my_client_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_client_ws_discovery)
        my_client_ws_discovery.start()

        # there a different methods to detect devices:
        # without specifying a type and a location, every WsDiscovery compatible device will be detected
        # (that can even be printers).
        services = my_client_ws_discovery.search_services(timeout=SEARCH_TIMEOUT,
                                                          scopes=ScopesType(self.my_location.scope_string))
        self.assertEqual(len(services), 1)  # both devices found

        my_consumer = SdcConsumer.from_wsd_service(services[0], ssl_context_container=None)
        self.my_consumers.append(my_consumer)
        my_consumer.start_all(not_subscribed_actions=periodic_actions_and_system_error_report)
        my_mdib = EntityConsumerMdib(my_consumer)
        my_mdib.init_mdib()

        # we want to set a patient.
        # first we must find the operation that has PatientContextDescriptor as operation target
        patient_context_entities = my_mdib.entities.by_node_type(pm.PatientContextDescriptor)
        self.assertEqual(len(patient_context_entities), 1)
        my_patient_context_entity = patient_context_entities[0]
        all_operation_entities = my_mdib.entities.by_node_type(pm.SetContextStateOperationDescriptor)
        my_operations = [op for op in all_operation_entities if
                         op.descriptor.OperationTarget == my_patient_context_entity.handle]
        self.assertEqual(len(my_operations), 1)
        my_operation = my_operations[0]

        # make a proposed new patient context:
        context_service = my_consumer.context_service_client
        proposed_patient = my_patient_context_entity.new_state()
        # The new state has  as a placeholder the descriptor handle as handle
        # => provider shall create a new state
        proposed_patient.Firstname = 'Jack'
        proposed_patient.Lastname = 'Miller'
        future = context_service.set_context_state(operation_handle=my_operation.handle,
                                                   proposed_context_states=[proposed_patient])
        result = future.result(timeout=5)
        self.assertEqual(result.InvocationInfo.InvocationState, msg_types.InvocationState.FINISHED)
        my_patient_context_entity.update()
        # provider should have replaced the placeholder handle with a new one.
        self.assertFalse(proposed_patient.Handle in my_patient_context_entity.states)

    def test_operation_handler(self):
        """The example shows how to implement own handlers for operations.

        It shows multiple ways how a client can find the desired operation.
        """
        # Create a device like in the examples above, but provide an own role provider.
        # This role provider is used instead of the default one.
        my_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_ws_discovery)
        my_ws_discovery.start()

        specific_components = SdcProviderComponents(role_provider_class=MyProductImpl)
        # use the minimalistic mdib from reference test:
        mdib_path = os.path.join(here, '../examples/ReferenceTest/reference_mdib.xml')  # noqa: PTH118
        my_generic_provider = create_generic_provider(my_ws_discovery,
                                                      self.my_location,
                                                      mdib_path,
                                                      specific_components=specific_components)

        self.my_providers.append(my_generic_provider)

        # connect a consumer to this provider:
        my_client_ws_discovery = WSDiscovery('127.0.0.1')
        self.my_ws_discoveries.append(my_client_ws_discovery)
        my_client_ws_discovery.start()

        services = my_client_ws_discovery.search_services(timeout=SEARCH_TIMEOUT,
                                                          scopes=ScopesType(self.my_location.scope_string))
        self.assertEqual(len(services), 1)

        self.service = SdcConsumer.from_wsd_service(services[0], ssl_context_container=None)
        my_consumer = self.service
        self.my_consumers.append(my_consumer)
        my_consumer.start_all(not_subscribed_actions=periodic_actions_and_system_error_report)
        my_mdib = EntityConsumerMdib(my_consumer)
        my_mdib.init_mdib()

        sco_handle = 'sco.mds0'
        my_product_impl = my_generic_provider.product_lookup[sco_handle]
        # call activate operation:
        # A client should NEVER! use the handle of the operation directly, always use the code(s) to identify things.
        # Handles are random values without any meaning, they are only unique id's in the mdib.
        operation_entities = my_mdib.entities.by_coding(MY_CODE_1.coding)
        # the mdib contains 2 operations with the same code. To keep things simple, just use the first one here.
        self._logger.info('looking for operations with code %r', MY_CODE_1.coding)
        op_entity = operation_entities[0]
        argument = 'foo'
        self._logger.info('calling operation %s, argument = %r', op_entity.handle, argument)
        future = my_consumer.set_service_client.activate(op_entity.handle, arguments=[argument])
        result = future.result()
        print(result)
        self.assertEqual(my_product_impl.my_provider_1.operation1_called, 1)
        args = my_product_impl.my_provider_1.operation1_args
        self.assertEqual(1, len(args))
        self.assertEqual(args[0].ArgValue, 'foo')

        # call set_string operation
        sco_handle = 'sco.vmd1.mds0'
        my_product_impl = my_generic_provider.product_lookup[sco_handle]

        self._logger.info('looking for operations with code %r', MY_CODE_2.coding)
        op_entities = my_mdib.entities.by_coding(MY_CODE_2.coding)
        my_op = op_entities[0]
        for value in ('foo', 'bar'):
            self._logger.info('calling operation %s, argument = %r', my_op.handle, value)
            future = my_consumer.set_service_client.set_string(my_op.handle, value)
            result = future.result()
            print(result)
            time.sleep(1)
            self.assertEqual(my_product_impl.my_provider_1.operation2_args, value)
            op_target_entity = my_mdib.entities.by_handle(my_op.descriptor.OperationTarget)
            self.assertEqual(op_target_entity.state.MetricValue.Value, value)
        self.assertEqual(my_product_impl.my_provider_1.operation2_called, 2)

        # call setValue operation
        op_target_entities = my_mdib.entities.by_coding(MY_CODE_3_TARGET.coding)
        op_target_entity = op_target_entities[0]

        all_operations = my_mdib.entities.by_node_type(pm.SetValueOperationDescriptor)
        my_ops = [op for op in all_operations if op.descriptor.OperationTarget == op_target_entity.handle]

        future = my_consumer.set_service_client.set_numeric_value(my_ops[0].handle, Decimal('42'))
        result = future.result()
        print(result)
        self.assertEqual(my_product_impl.my_provider_2.operation3_args, 42)
        ent = my_mdib.entities.by_handle(op_target_entity.handle)
        self.assertEqual(ent.state.MetricValue.Value, 42)
