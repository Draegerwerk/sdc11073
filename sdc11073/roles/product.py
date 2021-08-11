from . import alarmprovider
from . import audiopauseprovider
from . import clockprovider
from . import contextprovider
from . import metricprovider
from . import operationprovider
from . import patientcontextprovider
from . import providerbase
from .. import loghelper
from .. import namespaces


class GenericSetComponentStateOperationProvider(providerbase.ProviderRole):
    """
    Responsible for SetComponentState Operations
    """

    def make_operation_instance(self, operation_descriptor_container, operations_factory):
        """ Can handle following cases:
        SetComponentStateOperationDescriptor, target = any AbstractComponentDescriptor: => handler = _set_component_state
        """
        operation_target_handle = operation_descriptor_container.OperationTarget
        op_target_descriptor_container = self._mdib.descriptions.handle.get_one(operation_target_handle)

        if operation_descriptor_container.NODETYPE == namespaces.domTag('SetComponentStateOperationDescriptor'):
            if op_target_descriptor_container.NODETYPE in (namespaces.domTag('MdsDescriptor'),
                                                           namespaces.domTag('ChannelDescriptor'),
                                                           namespaces.domTag('VmdDescriptor'),
                                                           namespaces.domTag('ClockDescriptor'),
                                                           namespaces.domTag('ScoDescriptor'),
                                                           ):
                op_cls = operations_factory(namespaces.domTag('SetComponentStateOperationDescriptor'))
                operation = self._mk_operation(op_cls,
                                               handle=operation_descriptor_container.handle,
                                               operation_target_handle=operation_target_handle,
                                               coded_value=operation_descriptor_container.Type,
                                               current_argument_handler=self._set_component_state)
                return operation
        elif operation_descriptor_container.NODETYPE == namespaces.domTag('ActivateOperationDescriptor'):
            #  on what can activate be called?
            if op_target_descriptor_container.NODETYPE in (namespaces.domTag('MdsDescriptor'),
                                                           namespaces.domTag('ChannelDescriptor'),
                                                           namespaces.domTag('VmdDescriptor'),
                                                           namespaces.domTag('ScoDescriptor'),
                                                           ):
                # no generic handler to be called!
                op_cls = operations_factory(namespaces.domTag('ActivateOperationDescriptor'))
                return self._mk_operation(op_cls,
                                          handle=operation_descriptor_container.handle,
                                          operation_target_handle=operation_target_handle,
                                          coded_value=operation_descriptor_container.Type)
        return None

    def _set_component_state(self, operation_instance, value):
        """

        :param operation_instance: the operation
        :param value: a list of proposed metric states
        :return:
        """
        # ToDo: consider ModifiableDate attribute
        operation_instance.current_value = value
        with self._mdib.transaction_manager() as mgr:
            for proposed_state in value:
                state = mgr.get_state(proposed_state.descriptorHandle)
                if state.isComponentState:
                    self._logger.info('updating {} with proposed component state', state)
                    state.update_from_other_container(proposed_state,
                                                      skipped_properties=['StateVersion', 'DescriptorVersion'])
                else:
                    self._logger.warn('_set_component_state operation: ignore invalid referenced type {} in operation',
                                      state.NODETYPE)


class BaseProduct:
    def __init__(self, log_prefix):
        # self.contextstate_provider = contextprovider.GenericContextProvider(log_prefix=log_prefix)  # default handler
        # self.componentstate_provider = GenericSetComponentStateOperationProvider(log_prefix=log_prefix) # default handler
        # self.metric_provider = None
        # self.patientcontext_provider = None
        # self.alarm_provider = None
        # self.alarm_provider = None
        # self.operation_provider = None
        # self.audiopause_provider = None
        # self.daynight_provider = None
        # self.clock_provider = None
        # self.ensembleContextProvider = None
        self._mdib = None
        self._ordered_providers = []  # order matters, each provider can hide operations of later ones
        # start with most specific providers, end with most general ones
        self._logger = loghelper.get_logger_adapter('sdc.device.{}'.format(self.__class__.__name__), log_prefix)

    def _all_providers_sorted(self):
        return self._ordered_providers
        # # specialized roles first, generic roles last
        # return self._without_none_values([self.audiopause_provider, self.daynight_provider, self.clock_provider,
        #         self.patientcontext_provider, self.alarm_provider,
        #         self.metric_provider, self.ensembleContextProvider, self.operation_provider,
        #         self.contextstate_provider,
        #         self.componentstate_provider])

    @staticmethod
    def _without_none_values(some_list):
        return [e for e in some_list if e is not None]

    def init_operations(self, mdib, sco):
        """ register all actively provided operations """
        self._mdib = mdib
        for role_handler in self._all_providers_sorted():
            role_handler.init_operations(mdib)

        self._register_existing_mdib_operations(sco)

        for role_handler in self._all_providers_sorted():
            operations = role_handler.make_missing_operations(sco.operations_factory)
            for operation in operations:
                sco.register_operation(operation)

        # log all operations that do not have a handler now
        all_mdib_ops = []
        for nodetype in [namespaces.domTag('SetValueOperationDescriptorContainer'),
                         namespaces.domTag('SetStringOperationDescriptor'),
                         namespaces.domTag('SetContextStateOperationDescriptorContainer'),
                         namespaces.domTag('SetMetricStateOperationDescriptorContainer'),
                         namespaces.domTag('SetComponentStateOperationDescriptorContainer'),
                         namespaces.domTag('SetAlertStateOperationDescriptorContainer'),
                         namespaces.domTag('ActivateOperationDescriptorContainer')]:
            all_mdib_ops.extend(self._mdib.descriptions.NODETYPE.get(nodetype, []))
        all_mdib_op_handles = [op.Handle for op in all_mdib_ops]
        all_not_registered_op_handles = [op_h for op_h in all_mdib_op_handles if
                                         sco.get_operation_by_handle(op_h) is None]
        if not all_mdib_op_handles:
            self._logger.info('this device has no operations in mdib.')
        elif all_not_registered_op_handles:
            self._logger.info('there are operations without handler! handles = {}', all_not_registered_op_handles)
        else:
            self._logger.info('there are no operations without handler.')
        mdib.mk_state_containers_for_all_descriptors()
        mdib.pre_commit_handler = self._on_pre_commit
        mdib.post_commit_handler = self._on_post_commit

    def stop(self):
        for role_handler in self._all_providers_sorted():
            role_handler.stop()

    def make_operation_instance(self, operation_descriptor_container, operations_factory):
        """ try to get an operation for this operation_descriptor_container ( given in mdib) """
        operation_target_handle = operation_descriptor_container.OperationTarget
        operation_target_descr = self._mdib.descriptions.handle.get_one(operation_target_handle,
                                                                        allow_none=True)  # descriptor container
        if operation_target_descr is None:
            # this operation is incomplete, the operation target does not exist. Registration not possible.
            self._logger.warn(
                'Operation {}: target {} does not exist, will not register operation'.format(
                    operation_descriptor_container.handle, operation_target_handle))
            return None
        for role_handler in self._all_providers_sorted():
            operation = role_handler.make_operation_instance(operation_descriptor_container, operations_factory)
            if operation is not None:
                self._logger.info('{} provided operation for {}'.format(role_handler.__class__.__name__,
                                                                        operation_descriptor_container))
                return operation
            self._logger.debug(
                '{}: no handler for {}'.format(operation.__class__.__name__, operation_descriptor_container))
        return None

    def _register_existing_mdib_operations(self, sco):
        operation_descriptor_containers = self._mdib.get_operation_descriptors()
        for descriptor in operation_descriptor_containers:
            registered_op = sco.get_operation_by_handle(descriptor.handle)
            if registered_op is None:
                self._logger.info('found unregistered {} in mdib, handle={}, code={} target={}'.format(
                    descriptor.NODETYPE.localname, descriptor.Handle, descriptor.Type, descriptor.OperationTarget))
                operation = self.make_operation_instance(descriptor, sco.operations_factory)
                if operation is not None:
                    sco.register_operation(operation)

    def _on_pre_commit(self, mdib, transaction):
        for provider in self._all_providers_sorted():
            provider.on_pre_commit(mdib, transaction)
        self._add_missing_states_to_transaction(mdib, transaction)

    def _on_post_commit(self, mdib, transaction):
        for provider in self._all_providers_sorted():
            provider.on_post_commit(mdib, transaction)
        self._remove_states_for_deleted_descriptors(mdib, transaction)

    def _remove_states_for_deleted_descriptors(self, mdib, transaction):
        """
        remove states from mdib for deleted descriptors
        :return:
        """
        for tr_item in transaction.descriptor_updates.values():
            if tr_item.new is None:
                # deleted descriptor
                objects = mdib.states.descriptorHandle.get(tr_item.old.handle)
                if objects:
                    mdib.states.remove_objects(objects)
                objects = mdib.context_states.descriptorHandle.get(tr_item.old.handle)
                if objects:
                    mdib.context_states.remove_objects(objects)

    def _add_missing_states_to_transaction(self, mdib, transaction):
        """
        add states to new descriptors if they are not part of this transaction
        """
        for tr_item in transaction.descriptor_updates.values():
            if tr_item.old is None:
                # new descriptor
                state_cls = mdib.get_state_class_for_descriptor(tr_item.new)
                if not state_cls.isMultiState:
                    if not transaction.has_state(tr_item.new.handle):
                        state = state_cls(mdib.nsmapper, tr_item.new)
                        state.set_node_member()
                        transaction.add_state(state)


class GenericProduct(BaseProduct):
    def __init__(self, audiopause_provider, daynight_provider, clock_provider, log_prefix):
        super().__init__(log_prefix)

        self._ordered_providers.extend([audiopause_provider, daynight_provider, clock_provider])
        self._ordered_providers.extend([patientcontextprovider.GenericPatientContextProvider(log_prefix=log_prefix),
                                        alarmprovider.GenericAlarmProvider(log_prefix=log_prefix),
                                        metricprovider.GenericMetricProvider(log_prefix=log_prefix),
                                        operationprovider.OperationProvider(log_prefix=log_prefix),
                                        GenericSetComponentStateOperationProvider(log_prefix=log_prefix)
                                        ])


class MinimalProduct(BaseProduct):
    def __init__(self, log_prefix=None):
        super().__init__(log_prefix)
        self.metric_provider = metricprovider.GenericMetricProvider(log_prefix=log_prefix)  # needed in a test
        self._ordered_providers.extend([audiopauseprovider.GenericSDCAudioPauseProvider(log_prefix=log_prefix),
                                        clockprovider.GenericSDCClockProvider(log_prefix=log_prefix),
                                        patientcontextprovider.GenericPatientContextProvider(log_prefix=log_prefix),
                                        alarmprovider.GenericAlarmProvider(log_prefix=log_prefix),
                                        self.metric_provider,
                                        operationprovider.OperationProvider(log_prefix=log_prefix),
                                        GenericSetComponentStateOperationProvider(log_prefix=log_prefix)
                                        ])


class ExtendedProduct(MinimalProduct):
    def __init__(self, log_prefix=None):
        super().__init__(log_prefix)
        self._ordered_providers.extend([audiopauseprovider.GenericSDCAudioPauseProvider(log_prefix=log_prefix),
                                        clockprovider.GenericSDCClockProvider(log_prefix=log_prefix),
                                        contextprovider.EnsembleContextProvider(log_prefix=log_prefix),
                                        contextprovider.LocationContextProvider(log_prefix=log_prefix),
                                        patientcontextprovider.GenericPatientContextProvider(log_prefix=log_prefix),
                                        alarmprovider.GenericAlarmProvider(log_prefix=log_prefix),
                                        self.metric_provider,
                                        operationprovider.OperationProvider(log_prefix=log_prefix),
                                        GenericSetComponentStateOperationProvider(log_prefix=log_prefix)
                                        ])
