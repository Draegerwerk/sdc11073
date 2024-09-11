from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from contextlib import AbstractContextManager
    from sdc11073.definitions_base import BaseDefinitions, AbstractDataModel
    from .entityprotocol import ProviderEntityGetterProtocol
    from .transactionsprotocol import (DescriptorTransactionManagerProtocol,
                                       ContextStateTransactionManagerProtocol,
                                       StateTransactionManagerProtocol)


class ProviderMdibProtocol(Protocol):
    entities: ProviderEntityGetterProtocol
    sdc_definitions: type[BaseDefinitions]
    data_model: AbstractDataModel

    def descriptor_transaction(self) -> AbstractContextManager[DescriptorTransactionManagerProtocol]:
        ...

    def context_state_transaction(self) -> AbstractContextManager[ContextStateTransactionManagerProtocol]:
        ...

    def alert_state_transaction(self, set_determination_time: bool = True) \
            -> AbstractContextManager[StateTransactionManagerProtocol]:
        ...

    def metric_state_transaction(self, set_determination_time: bool = True) \
            -> AbstractContextManager[StateTransactionManagerProtocol]:
        ...

    def rt_sample_state_transaction(self, set_determination_time: bool = False) \
            -> AbstractContextManager[StateTransactionManagerProtocol]:
        ...

    def component_state_transaction(self) -> AbstractContextManager[StateTransactionManagerProtocol]:
        ...

    def operational_state_transaction(self) -> AbstractContextManager[StateTransactionManagerProtocol]:
        ...