from __future__ import annotations

import uuid
from collections import defaultdict
from contextlib import contextmanager
from threading import Lock
from typing import Type, TYPE_CHECKING, Optional

from . import mdibbase
from .devicemdibxtra import DeviceMdibMethods
from .transactions import RtDataMdibUpdateTransaction, MdibUpdateTransaction
from .. import loghelper
from ..definitions_base import ProtocolsRegistry
from ..definitions_sdc import SDC_v1_Definitions
from ..observableproperties import ObservableProperty
from ..pysoap.msgreader import MessageReader

if TYPE_CHECKING:
    from ..definitions_base import BaseDefinitions


class DeviceMdibContainer(mdibbase.MdibContainer):
    """Device side implementation of a mdib.
     Do not modify containers directly, use transactions for that purpose.
     Transactions keep track of changes and initiate sending of update notifications to clients."""
    transaction = ObservableProperty(fire_only_on_changed_value=False)
    rt_updates = ObservableProperty(fire_only_on_changed_value=False)  # different observable for performance

    def __init__(self,
                 sdc_definitions: Optional[Type[BaseDefinitions]] = None,
                 log_prefix: Optional[str] = None,
                 extras_cls=None,
                 transaction_cls = None
                 ):
        """
        :param sdc_definitions: defaults to sdc11073.definitions_sdc.SDC_v1_Definitions
        :param log_prefix: a string
        :param extras_cls: class for extra functionality
        :param transaction_cls: runs the transaction
        """
        if sdc_definitions is None:
            sdc_definitions = SDC_v1_Definitions
        super().__init__(sdc_definitions)
        if extras_cls is None:
            extras_cls = DeviceMdibMethods
        self._xtra = extras_cls(self)
        self._logger = loghelper.get_logger_adapter('sdc.device.mdib', log_prefix)
        self._tr_lock = Lock()  # transaction lock

        self.sequence_id = uuid.uuid4().urn  # this uuid identifies this mdib instance

        self._current_location = None
        self._annotators = {}
        self._current_transaction = None

        self.pre_commit_handler = None  # pre_commit_handler can modify transaction if needed before it is committed
        self.post_commit_handler = None  # post_commit_handler can modify mdib if needed after it is committed
        self._transaction_cls = transaction_cls or MdibUpdateTransaction
        self._retrievability_episodic = []  # a list of handles
        self.retrievability_periodic = defaultdict(list)

    @property
    def xtra(self):
        return self._xtra

    @contextmanager
    def transaction_manager(self, set_determination_time=True):
        # pylint: disable=protected-access
        with self._tr_lock:
            with self.mdib_lock:
                try:
                    self._current_transaction = self._transaction_cls(self, self.logger)
                    yield self._current_transaction
                    if callable(self.pre_commit_handler):
                        self.pre_commit_handler(self, self._current_transaction)  # pylint: disable=not-callable
                    if self._current_transaction._error:
                        self._logger.info('transaction_manager: transaction without updates!')
                    else:
                        processor = self._current_transaction.process_transaction(set_determination_time)
                        self.transaction = processor  # update observable
                        self._current_transaction.mdib_version = self.mdib_version

                        if callable(self.post_commit_handler):
                            self.post_commit_handler(self, self._current_transaction)  # pylint: disable=not-callable
                finally:
                    self._current_transaction = None

    @contextmanager
    def _rt_sample_transaction(self):
        with self._tr_lock:
            with self.mdib_lock:
                try:
                    self._current_transaction = RtDataMdibUpdateTransaction(self, self._logger)
                    yield self._current_transaction
                    if callable(self.pre_commit_handler):
                        self.pre_commit_handler(self, self._current_transaction)  # pylint: disable=not-callable
                    if self._current_transaction.error:
                        self._logger.info('_rt_sample_transaction: transaction without updates!')
                    else:
                        self._process_internal_rt_transaction()
                        if callable(self.post_commit_handler):
                            self.post_commit_handler(self, self._current_transaction)  # pylint: disable=not-callable
                finally:
                    self._current_transaction = None

    def _process_internal_rt_transaction(self):
        mgr = self._current_transaction
        # handle real time samples
        if len(mgr.rt_sample_state_updates) > 0:
            self.mdib_version += 1
            updates = []
            self._logger.debug('transaction_manager: rtSample updates = {}', mgr.rt_sample_state_updates)
            for transaction_item in mgr.rt_sample_state_updates.values():
                updates.append(transaction_item.new)
            # makes copies of all states for sending, so that they can't be affected by transactions after this one
            updates = [s.mk_copy(copy_node=False) for s in updates]
            self.rt_updates = updates
        mgr.mdib_version = self.mdib_version

    @classmethod
    def from_mdib_file(cls,
                       path: str,
                       protocol_definition: Optional[Type[BaseDefinitions]] = None,
                       xml_reader_class: Optional[Type[MessageReader]] = MessageReader,
                       log_prefix: Optional[str] = None):
        """
        An alternative constructor for the class
        :param path: the input file path for creating the mdib
        :param protocol_definition: an optional object derived from BaseDefinitions, forces usage of this definition
        :param xml_reader_class: class that is used to read mdib xml file
        :param log_prefix: a string or None
        :return: instance
        """
        with open(path, 'rb') as the_file:
            xml_text = the_file.read()
        return cls.from_string(xml_text,
                               protocol_definition,
                               xml_reader_class,
                               log_prefix)

    @classmethod
    def from_string(cls,
                    xml_text: bytes,
                    protocol_definition: Optional[Type[BaseDefinitions]] = None,
                    xml_reader_class: Optional[Type[MessageReader]] = MessageReader,
                    log_prefix: Optional[str] = None):
        """
        An alternative constructor for the class
        :param xml_text: the input string for creating the mdib
        :param protocol_definition: an optional object derived from BaseDefinitions, forces usage of this definition
        :param xml_reader_class: class that is used to read mdib xml file
        :param log_prefix: a string or None
        :return: instance
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

        xml_msg_reader = xml_reader_class(protocol_definition, None, mdib._logger)
        message_data = xml_msg_reader.read_payload_data(xml_text)
        descriptor_containers, state_containers = xml_msg_reader.read_get_mdib_response(message_data)

        mdib.add_description_containers(descriptor_containers)
        mdib.add_state_containers(state_containers)
        mdib.xtra.mk_state_containers_for_all_descriptors()
        mdib.xtra.update_retrievability_lists()
        mdib.xtra.set_all_source_mds()
        return mdib
