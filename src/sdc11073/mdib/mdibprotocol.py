"""The module defines the interface of a provider mdib."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from contextlib import AbstractContextManager

    from sdc11073.definitions_base import AbstractDataModel, BaseDefinitions

    from .entityprotocol import ProviderEntityGetterProtocol
    from .transactionsprotocol import (
        ContextStateTransactionManagerProtocol,
        DescriptorTransactionManagerProtocol,
        StateTransactionManagerProtocol,
    )


class ProviderMdibProtocol(Protocol): # pragma: no cover
    """The interface of a provider mdib.

    This interface only expects the ProviderEntityGetterProtocol.
    The old implementation with separate lookups for descriptors, states and context states
    is not part of this protocol.
    """

    entities: ProviderEntityGetterProtocol
    sdc_definitions: type[BaseDefinitions]
    data_model: AbstractDataModel

    def descriptor_transaction(self) -> AbstractContextManager[DescriptorTransactionManagerProtocol]:
        """Return a transaction."""

    def context_state_transaction(self) -> AbstractContextManager[ContextStateTransactionManagerProtocol]:
        """Return a transaction."""

    def alert_state_transaction(self, set_determination_time: bool = True) \
            -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction."""

    def metric_state_transaction(self, set_determination_time: bool = True) \
            -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction."""

    def rt_sample_state_transaction(self, set_determination_time: bool = False) \
            -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction."""

    def component_state_transaction(self) -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction."""

    def operational_state_transaction(self) -> AbstractContextManager[StateTransactionManagerProtocol]:
        """Return a transaction."""
