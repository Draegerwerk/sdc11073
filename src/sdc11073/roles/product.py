from . import alarmprovider
from . import clockprovider
from . import contextprovider
from . import metricprovider
from . import operationprovider
from . import patientcontextprovider
from . import providerbase
from .audiopauseprovider import AudioPauseProvider
from .. import loghelper


class GenericSetComponentStateOperationProvider(providerbase.ProviderRole):
    """
    Responsible for SetComponentState Operations
    """

    def make_operation_instance(self, operation_descriptor_container, operation_cls_getter):
        """ Can handle following cases:
        SetComponentStateOperationDescriptor, target = any AbstractComponentDescriptor: => handler = _set_component_state
        """
        pm_names = self._mdib.data_model.pm_names
        operation_target_handle = operation_descriptor_container.OperationTarget
        op_target_descriptor_container = self._mdib.descriptions.handle.get_one(operation_target_handle)

        if operation_descriptor_container.NODETYPE == pm_names.SetComponentStateOperationDescriptor:
            if op_target_descriptor_container.NODETYPE in (pm_names.MdsDescriptor,
                                                           pm_names.ChannelDescriptor,
                                                           pm_names.VmdDescriptor,
                                                           pm_names.ClockDescriptor,
                                                           pm_names.ScoDescriptor,
                                                           ):
                op_cls = operation_cls_getter(pm_names.SetComponentStateOperationDescriptor)
                operation = self._mk_operation(op_cls,
                                               handle=operation_descriptor_container.Handle,
                                               operation_target_handle=operation_target_handle,
                                               coded_value=operation_descriptor_container.Type,
                                               current_argument_handler=self._set_component_state)
                return operation
        elif operation_descriptor_container.NODETYPE == pm_names.ActivateOperationDescriptor:
            #  on what can activate be called?
            if op_target_descriptor_container.NODETYPE in (pm_names.MdsDescriptor,
                                                           pm_names.ChannelDescriptor,
                                                           pm_names.VmdDescriptor,
                                                           pm_names.ScoDescriptor,
                                                           ):
                # no generic handler to be called!
                op_cls = operation_cls_getter(pm_names.ActivateOperationDescriptor)
                return self._mk_operation(op_cls,
                                          handle=operation_descriptor_container.Handle,
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
                state = mgr.get_state(proposed_state.DescriptorHandle)
                if state.is_component_state:
                    self._logger.info('updating {} with proposed component state', state)
                    state.update_from_other_container(proposed_state,
                                                      skipped_properties=['StateVersion', 'DescriptorVersion'])
                else:
                    self._logger.warn('_set_component_state operation: ignore invalid referenced type {} in operation',
                                      state.NODETYPE)


class BaseProduct:
    """A Product is associated to a single sco. If a mdib contains multiple sco instances,
    there will be multiple Products."""

    def __init__(self, mdib, sco, log_prefix):
        """

        :param mdib: the device mdib
        'param sco: sco of device
        :param log_prefix: str
        """
        self._sco = sco
        self._mdib = mdib
        self._model = mdib.data_model
        self._ordered_providers = []  # order matters, each provider can hide operations of later ones
        # start with most specific providers, end with most general ones
        self._logger = loghelper.get_logger_adapter(f'sdc.device.{self.__class__.__name__}', log_prefix)

    def _all_providers_sorted(self):
        return self._ordered_providers

    @staticmethod
    def _without_none_values(some_list):
        return [e for e in some_list if e is not None]

    def init_operations(self):
        """ register all actively provided operations """
        pm_names = self._model.pm_names
        sco_handle = self._sco.sco_descriptor_container.Handle
        self._logger.info('init_operations for sco {}.', sco_handle)

        for role_handler in self._all_providers_sorted():
            role_handler.init_operations(self._sco)

        self._register_existing_mdib_operations(self._sco)

        for role_handler in self._all_providers_sorted():
            operations = role_handler.make_missing_operations(self._sco)
            if operations:
                info = ', '.join([f'{op.OP_DESCR_QNAME.localname} {op.handle}' for op in operations])
                self._logger.info('role handler {} added operations to mdib: {}',
                                  role_handler.__class__.__name__, info)
            for operation in operations:
                self._sco.register_operation(operation)

        all_sco_operations = self._mdib.descriptions.parent_handle.get(self._sco.sco_descriptor_container.Handle, [])
        all_op_handles = [op.Handle for op in all_sco_operations]
        all_not_registered_op_handles = [op_h for op_h in all_op_handles if
                                         self._sco.get_operation_by_handle(op_h) is None]

        if not all_op_handles:
            self._logger.info('sco {} has no operations in mdib.', sco_handle)
        elif all_not_registered_op_handles:
            self._logger.info('sco {} has operations without handler! handles = {}',
                              sco_handle, all_not_registered_op_handles)
        else:
            self._logger.info('sco {}: all operations have a handler.', sco_handle)
        self._mdib.xtra.mk_state_containers_for_all_descriptors()
        self._mdib.pre_commit_handler = self._on_pre_commit
        self._mdib.post_commit_handler = self._on_post_commit

    def stop(self):
        for role_handler in self._all_providers_sorted():
            role_handler.stop()

    def make_operation_instance(self, operation_descriptor_container, operation_cls_getter):
        """ try to get an operation for this operation_descriptor_container ( given in mdib) """
        operation_target_handle = operation_descriptor_container.OperationTarget
        operation_target_descr = self._mdib.descriptions.handle.get_one(operation_target_handle,
                                                                        allow_none=True)  # descriptor container
        if operation_target_descr is None:
            # this operation is incomplete, the operation target does not exist. Registration not possible.
            self._logger.warn(
                f'Operation {operation_descriptor_container.Handle}: '
                f'target {operation_target_handle} does not exist, will not register operation')
            return None
        for role_handler in self._all_providers_sorted():
            operation = role_handler.make_operation_instance(operation_descriptor_container, operation_cls_getter)
            if operation is not None:
                self._logger.debug(
                    f'{role_handler.__class__.__name__} provided operation for {operation_descriptor_container}')
                return operation
            self._logger.debug(f'{role_handler.__class__.__name__}: no handler for {operation_descriptor_container}')
        return None

    def _register_existing_mdib_operations(self, sco):
        operation_descriptor_containers = self._mdib.descriptions.parent_handle.get(
            self._sco.sco_descriptor_container.Handle, [])
        for descriptor in operation_descriptor_containers:
            registered_op = sco.get_operation_by_handle(descriptor.Handle)
            if registered_op is None:
                self._logger.debug(
                    f'found unregistered {descriptor.NODETYPE.localname} in mdib, handle={descriptor.Handle}, '
                    f'code={descriptor.Type} target={descriptor.OperationTarget}')
                operation = self.make_operation_instance(descriptor, sco.operation_cls_getter)
                if operation is not None:
                    sco.register_operation(operation)

    def _on_pre_commit(self, mdib, transaction):
        for provider in self._all_providers_sorted():
            provider.on_pre_commit(mdib, transaction)

    def _on_post_commit(self, mdib, transaction):
        for provider in self._all_providers_sorted():
            provider.on_post_commit(mdib, transaction)


class GenericProduct(BaseProduct):
    def __init__(self, mdib, sco, audio_pause_provider, day_night_provider, clock_provider, log_prefix):
        super().__init__(mdib, sco, log_prefix)

        self._ordered_providers.extend([audio_pause_provider, day_night_provider, clock_provider])
        self._ordered_providers.extend(
            [patientcontextprovider.GenericPatientContextProvider(mdib, log_prefix=log_prefix),
             alarmprovider.GenericAlarmProvider(mdib, log_prefix=log_prefix),
             metricprovider.GenericMetricProvider(mdib, log_prefix=log_prefix),
             operationprovider.OperationProvider(mdib, log_prefix=log_prefix),
             GenericSetComponentStateOperationProvider(mdib, log_prefix=log_prefix)
             ])


class MinimalProduct(BaseProduct):
    def __init__(self, mdib, sco, log_prefix=None):
        super().__init__(mdib, sco, log_prefix)
        self.metric_provider = metricprovider.GenericMetricProvider(mdib, log_prefix=log_prefix)  # needed in a test
        self._ordered_providers.extend([AudioPauseProvider(mdib, log_prefix=log_prefix),
                                        clockprovider.GenericSDCClockProvider(mdib, log_prefix=log_prefix),
                                        patientcontextprovider.GenericPatientContextProvider(mdib,
                                                                                             log_prefix=log_prefix),
                                        alarmprovider.GenericAlarmProvider(mdib, log_prefix=log_prefix),
                                        self.metric_provider,
                                        operationprovider.OperationProvider(mdib, log_prefix=log_prefix),
                                        GenericSetComponentStateOperationProvider(mdib, log_prefix=log_prefix)
                                        ])


class ExtendedProduct(MinimalProduct):
    def __init__(self, mdib, sco, log_prefix=None):
        super().__init__(mdib, sco, log_prefix)
        self._ordered_providers.extend([AudioPauseProvider(mdib, log_prefix=log_prefix),
                                        clockprovider.GenericSDCClockProvider(mdib, log_prefix=log_prefix),
                                        contextprovider.EnsembleContextProvider(mdib, log_prefix=log_prefix),
                                        contextprovider.LocationContextProvider(mdib, log_prefix=log_prefix),
                                        patientcontextprovider.GenericPatientContextProvider(mdib,
                                                                                             log_prefix=log_prefix),
                                        alarmprovider.GenericAlarmProvider(mdib, log_prefix=log_prefix),
                                        self.metric_provider,
                                        operationprovider.OperationProvider(mdib, log_prefix=log_prefix),
                                        GenericSetComponentStateOperationProvider(mdib, log_prefix=log_prefix)
                                        ])
