from __future__ import annotations

import uuid
from collections import defaultdict
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING, Any, Callable

from sdc11073 import loghelper
from sdc11073.definitions_base import ProtocolsRegistry
from sdc11073.loghelper import LoggerAdapter
from sdc11073.observableproperties import ObservableProperty
from sdc11073.pysoap.msgreader import MessageReader

from . import mdibbase
from .providermdibxtra import ProviderMdibMethods
from .transactions import mk_transaction
from .transactionsprotocol import AnyTransactionManagerProtocol, TransactionType

if TYPE_CHECKING:
    from sdc11073.definitions_base import BaseDefinitions

    from .transactionsprotocol import (
        ContextStateTransactionManagerProtocol,
        DescriptorTransactionManagerProtocol,
        StateTransactionManagerProtocol,
        TransactionResultProtocol
    )

TransactionFactory = Callable[[mdibbase.MdibBase, TransactionType, LoggerAdapter],
                              AnyTransactionManagerProtocol]


class ProviderMdib(mdibbase.MdibBase):
    """Device side implementation of a mdib.

    Do not modify containers directly, use transactions for that purpose.
    Transactions keep track of changes and initiate sending of update notifications to clients.
    """

    transaction: TransactionResultProtocol | None = ObservableProperty(fire_only_on_changed_value=False)
    rt_updates = ObservableProperty(fire_only_on_changed_value=False)  # different observable for performance

    def __init__(self,
                 sdc_definitions: type[BaseDefinitions] | None = None,
                 log_prefix: str | None = None,
                 extra_functionality: type | None = None,
                 transaction_factory: TransactionFactory | None = None,
                 ):
        """Construct a ProviderMdib.

        :param sdc_definitions: defaults to sdc11073.definitions_sdc.SdcV1Definitions
        :param log_prefix: a string
        :param extra_functionality: class for extra functionality, default is ProviderMdibMethods
        :param transaction_factory: optional alternative transactions factory.
        """
        if sdc_definitions is None:
            from sdc11073.definitions_sdc import SdcV1Definitions  # lazy import, needed to brake cyclic imports
            sdc_definitions = SdcV1Definitions
        super().__init__(sdc_definitions,
                         loghelper.get_logger_adapter('sdc.device.mdib', log_prefix))
        if extra_functionality is None:
            extra_functionality = ProviderMdibMethods
        self._xtra = extra_functionality(self)
        self._tr_lock = Lock()  # transaction lock

        self.sequence_id = uuid.uuid4().urn  # this uuid identifies this mdib instance

        self._annotators = {}
        self.current_transaction = None

        self.pre_commit_handler = None  # pre_commit_handler can modify transaction if needed before it is committed
        self.post_commit_handler = None  # post_commit_handler can modify mdib if needed after it is committed
        self._transaction_factory = transaction_factory or mk_transaction
        self._retrievability_episodic = []  # a list of handles
        self.retrievability_periodic = defaultdict(list)

    @property
    def xtra(self) -> Any:
        """Give access to extended functionality."""
        return self._xtra

    @contextmanager
    def _transaction_manager(self,
                             transaction_type: TransactionType,
                             set_determination_time: bool = True) -> AbstractContextManager[
        AnyTransactionManagerProtocol]:
        """Start a transaction, return a new transaction manager."""
        with self._tr_lock, self.mdib_lock:
            try:
                self.current_transaction = self._transaction_factory(self, transaction_type, self.logger)
                yield self.current_transaction

                if callable(self.pre_commit_handler):
                    self.pre_commit_handler(self, self.current_transaction)
                if self.current_transaction.error:
                    self._logger.info('transaction_manager: transaction without updates!')
                else:
                    # update observables
                    transaction_result = self.current_transaction.process_transaction(set_determination_time)
                    self.transaction = transaction_result

                    if transaction_result.alert_updates:
                        self.alert_by_handle = {st.DescriptorHandle: st for st in transaction_result.alert_updates}
                    if transaction_result.comp_updates:
                        self.component_by_handle = {st.DescriptorHandle: st for st in transaction_result.comp_updates}
                    if transaction_result.ctxt_updates:
                        self.context_by_handle = {st.Handle: st for st in transaction_result.ctxt_updates}
                    if transaction_result.descr_created:
                        self.new_descriptors_by_handle = {descr.Handle: descr for descr
                                                          in transaction_result.descr_created}
                    if transaction_result.descr_deleted:
                        self.deleted_descriptors_by_handle = {descr.Handle: descr for descr
                                                              in transaction_result.descr_deleted}
                    if transaction_result.descr_updated:
                        self.updated_descriptors_by_handle = {descr.Handle: descr for descr
                                                              in transaction_result.descr_updated}
                    if transaction_result.metric_updates:
                        self.metrics_by_handle = {st.DescriptorHandle: st for st in transaction_result.metric_updates}
                    if transaction_result.op_updates:
                        self.operation_by_handle = {st.DescriptorHandle: st for st in transaction_result.op_updates}
                    if transaction_result.rt_updates:
                        self.waveform_by_handle = {st.DescriptorHandle: st for st in transaction_result.rt_updates}


                    if callable(self.post_commit_handler):
                        self.post_commit_handler(self, self.current_transaction)
            finally:
                self.current_transaction = None

    @contextmanager
    def context_state_transaction(self) -> AbstractContextManager[ContextStateTransactionManagerProtocol]:
        """Return a transaction for context state updates."""
        with self._transaction_manager(TransactionType.context) as mgr:
            yield mgr

    @contextmanager
    def alert_state_transaction(self, set_determination_time: bool = True) \
            -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction for alert state updates."""
        with self._transaction_manager(TransactionType.alert, set_determination_time) as mgr:
            yield mgr

    @contextmanager
    def metric_state_transaction(self, set_determination_time: bool = True) \
            -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction for metric state updates (not real time samples!)."""
        with self._transaction_manager(TransactionType.metric, set_determination_time) as mgr:
            yield mgr

    @contextmanager
    def rt_sample_state_transaction(self, set_determination_time: bool = False) \
            -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction for real time sample state updates."""
        with self._transaction_manager(TransactionType.rt_sample, set_determination_time) as mgr:
            yield mgr

    @contextmanager
    def component_state_transaction(self) -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction for component state updates."""
        with self._transaction_manager(TransactionType.component) as mgr:
            yield mgr

    @contextmanager
    def operational_state_transaction(self) -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction for operational state updates."""
        with self._transaction_manager(TransactionType.operational) as mgr:
            yield mgr

    @contextmanager
    def descriptor_transaction(self) -> AbstractContextManager[DescriptorTransactionManagerProtocol]:
        """Return a transaction for descriptor updates.

        This transaction also allows to handle the states that relate to the modified descriptors.
        """
        with self._transaction_manager(TransactionType.descriptor) as mgr:
            yield mgr

    @classmethod
    def from_mdib_file(cls,
                       path: str,
                       protocol_definition: type[BaseDefinitions] | None = None,
                       xml_reader_class: type[MessageReader] | None = MessageReader,
                       log_prefix: str | None = None) -> ProviderMdib:
        """Construct mdib from a file.

        :param path: the input file path for creating the mdib
        :param protocol_definition: an optional object derived from BaseDefinitions, forces usage of this definition
        :param xml_reader_class: class that is used to read mdib xml file
        :param log_prefix: a string or None
        :return: instance.
        """
        with Path(path).open('rb') as the_file:
            xml_text = the_file.read()
        return cls.from_string(xml_text,
                               protocol_definition,
                               xml_reader_class,
                               log_prefix)

    @classmethod
    def from_string(cls,
                    xml_text: bytes,
                    protocol_definition: type[BaseDefinitions] | None = None,
                    xml_reader_class: type[MessageReader] | None = MessageReader,
                    log_prefix: str | None = None) -> ProviderMdib:
        """Construct mdib from a string.

        :param xml_text: the input string for creating the mdib
        :param protocol_definition: an optional object derived from BaseDefinitions, forces usage of this definition
        :param xml_reader_class: class that is used to read mdib xml file
        :param log_prefix: a string or None
        :return: instance.
        """
        # get protocol definition that matches xml_text
        if protocol_definition is None:
            for definition_cls in ProtocolsRegistry.protocols:
                pm_namespace = definition_cls.data_model.ns_helper.PM.namespace.encode('utf-8')
                if pm_namespace in xml_text:
                    protocol_definition = definition_cls
                    break
        if protocol_definition is None:
            raise ValueError('cannot create instance, no known BICEPS schema version identified')
        mdib = cls(protocol_definition, log_prefix=log_prefix)

        xml_msg_reader = xml_reader_class(protocol_definition, None, mdib.logger)
        descriptor_containers, state_containers = xml_msg_reader.read_mdib_xml(xml_text)

        mdib.add_description_containers(descriptor_containers)
        mdib.add_state_containers(state_containers)
        mdib.xtra.mk_state_containers_for_all_descriptors()
        mdib.xtra.update_retrievability_lists()
        mdib.xtra.set_all_source_mds()
        return mdib
