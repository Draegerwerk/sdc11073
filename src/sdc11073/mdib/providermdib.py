from __future__ import annotations

import uuid
from collections import defaultdict
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from threading import Lock
from typing import TYPE_CHECKING, Any

from sdc11073 import loghelper
from sdc11073.definitions_base import ProtocolsRegistry
from sdc11073.observableproperties import ObservableProperty
from sdc11073.pysoap.msgreader import MessageReader

from . import mdibbase
from .providermdibxtra import ProviderMdibMethods
from .transactions import MdibUpdateTransaction, RtDataMdibUpdateTransaction

if TYPE_CHECKING:
    from sdc11073.definitions_base import BaseDefinitions

    from .transactions import TransactionManagerProtocol


class ProviderMdib(mdibbase.MdibBase):
    """Device side implementation of a mdib.

    Do not modify containers directly, use transactions for that purpose.
    Transactions keep track of changes and initiate sending of update notifications to clients.
    """

    transaction = ObservableProperty(fire_only_on_changed_value=False)
    rt_updates = ObservableProperty(fire_only_on_changed_value=False)  # different observable for performance

    def __init__(self,
                 sdc_definitions: type[BaseDefinitions] | None = None,
                 log_prefix: str | None = None,
                 extra_functionality: type | None = None,
                 transaction_cls: type[TransactionManagerProtocol] | None = None,
                 ):
        """Construct a ProviderMdib.

        :param sdc_definitions: defaults to sdc11073.definitions_sdc.SDC_v1_Definitions
        :param log_prefix: a string
        :param extra_functionality: class for extra functionality, default is ProviderMdibMethods
        :param transaction_cls: runs the transaction, default is MdibUpdateTransaction
        """
        if sdc_definitions is None:
            from sdc11073.definitions_sdc import SDC_v1_Definitions  # lazy import, needed to brake cyclic imports
            sdc_definitions = SDC_v1_Definitions
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
        self._transaction_cls = transaction_cls or MdibUpdateTransaction
        self._retrievability_episodic = []  # a list of handles
        self.retrievability_periodic = defaultdict(list)

    @property
    def xtra(self) -> Any:
        """Give access to extended functionality."""
        return self._xtra

    @contextmanager
    def transaction_manager(self, set_determination_time: bool = True) -> AbstractContextManager[
        TransactionManagerProtocol]:
        """Start a transaction, return a new transaction manager."""
        with self._tr_lock, self.mdib_lock:
            try:
                self.current_transaction = self._transaction_cls(self, self.logger)
                yield self.current_transaction

                if callable(self.pre_commit_handler):
                    self.pre_commit_handler(self, self.current_transaction)
                if self.current_transaction.error:
                    self._logger.info('transaction_manager: transaction without updates!')
                else:
                    processor = self.current_transaction.process_transaction(set_determination_time)
                    self.transaction = processor  # update observable
                    self.current_transaction.mdib_version = self.mdib_version

                    if callable(self.post_commit_handler):
                        self.post_commit_handler(self, self.current_transaction)
            finally:
                self.current_transaction = None

    @contextmanager
    def _rt_sample_transaction(self):
        with self._tr_lock, self.mdib_lock:
            try:
                self.current_transaction = RtDataMdibUpdateTransaction(self, self._logger)
                yield self.current_transaction
                if callable(self.pre_commit_handler):
                    self.pre_commit_handler(self, self.current_transaction)
                if self.current_transaction.error:
                    self._logger.info('_rt_sample_transaction: transaction without updates!')
                else:
                    self._process_internal_rt_transaction()
                    if callable(self.post_commit_handler):
                        self.post_commit_handler(self, self.current_transaction)
            finally:
                self.current_transaction = None

    def _process_internal_rt_transaction(self):
        mgr = self.current_transaction
        # handle real time samples
        if len(mgr.rt_sample_state_updates) > 0:
            self.mdib_version += 1
            updates = []
            self._logger.debug('transaction_manager: rtSample updates = {}',  # noqa: PLE1205
                               mgr.rt_sample_state_updates)
            for transaction_item in mgr.rt_sample_state_updates.values():
                updates.append(transaction_item.new)
            # makes copies of all states for sending, so that they can't be affected by transactions after this one
            updates = [s.mk_copy(copy_node=False) for s in updates]
            self.rt_updates = updates
        mgr.mdib_version = self.mdib_version

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
