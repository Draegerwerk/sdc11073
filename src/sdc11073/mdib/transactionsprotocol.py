from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Protocol, Union

from .statecontainers import AbstractMultiStateProtocol, AbstractStateProtocol

if TYPE_CHECKING:

    from .descriptorcontainers import AbstractDescriptorProtocol


class TransactionType(Enum):
    """The different kinds of transactions.

    Each type results in a different report.
    """

    descriptor = 1
    metric = 2
    alert = 3
    component = 4
    context = 5
    operational = 6
    rt_sample = 7


class TransactionResultProtocol(Protocol):
    """TransactionResult contains all state and descriptors that were modified in the transaction.

    The states and descriptors are used to create the notification(s) that keep the consumers up to date.
    """

    descr_updated: list[AbstractDescriptorProtocol]
    descr_created: list[AbstractDescriptorProtocol]
    descr_deleted: list[AbstractDescriptorProtocol]
    metric_updates: list[AbstractStateProtocol]
    alert_updates: list[AbstractStateProtocol]
    comp_updates = list[AbstractStateProtocol]
    ctxt_updates = list[AbstractMultiStateProtocol]
    op_updates = list[AbstractStateProtocol]
    rt_updates = list[AbstractStateProtocol]

    has_descriptor_updates: bool

    def all_states(self) -> list[AbstractStateProtocol]:
        """Return all states in this transaction."""

class TransactionItemProtocol(Protocol):
    """A container for the old and the new version of a state or descriptor.

    If old is None, this is an object that is added to mdib.
    If new is None, this is an object that is deleted from mdib.
    If neither old nor new is None, this is an update to an existing object.
    """

    old: AbstractStateProtocol | AbstractDescriptorProtocol | None
    new: AbstractStateProtocol | AbstractDescriptorProtocol | None


@dataclass(frozen=True)
class TransactionItem:
    """Transaction Item with old and new container."""

    old: AbstractStateProtocol | AbstractDescriptorProtocol | None
    new: AbstractStateProtocol | AbstractDescriptorProtocol | None


class AbstractTransactionManagerProtocol(Protocol):
    """Interface of a TransactionManager."""

    new_mdib_version: int

    def process_transaction(self, set_determination_time: bool) -> TransactionResultProtocol:
        """Process the transaction."""

    # member variables that are available during a running transaction
    descriptor_updates: dict[str, TransactionItem]
    metric_state_updates: dict[str, TransactionItem]
    alert_state_updates: dict[str, TransactionItem]
    component_state_updates: dict[str, TransactionItem]
    context_state_updates: dict[str, TransactionItem]
    operational_state_updates: dict[str, TransactionItem]
    rt_sample_state_updates: dict[str, TransactionItem]
    error: bool


class DescriptorTransactionManagerProtocol(AbstractTransactionManagerProtocol):
    """Interface of a TransactionManager that modifies descriptors."""

    def actual_descriptor(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Look for new or updated descriptor in current transaction and in mdib."""

    def add_descriptor(self,
                       descriptor_container: AbstractDescriptorProtocol,
                       adjust_descriptor_version: bool = True,
                       state_container: AbstractStateProtocol | None = None):
        """Add a new descriptor to mdib."""

    def remove_descriptor(self, descriptor_handle: str):
        """Remove existing descriptor from mdib."""

    def get_descriptor(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Get a descriptor from mdib."""

    def has_state(self, descriptor_handle: str) -> bool:
        """Check if transaction has a state with given handle."""

    def get_state_transaction_item(self, handle: str) -> TransactionItemProtocol | None:
        """If transaction has a state with given handle, return the transaction-item, otherwise None."""

    def add_state(self, state_container: AbstractStateProtocol, adjust_state_version: bool = True):
        """Add a new state to mdib."""

    def unget_state(self, state_container: AbstractStateProtocol):
        """Forget a state that was provided before by a get_state or add_state call."""

    def get_state(self, descriptor_handle: str) -> AbstractStateProtocol:
        """Read a state from mdib and add it to the transaction."""

    def get_context_state(self, context_state_handle: str) -> AbstractMultiStateProtocol:
        """Read a ContextState from mdib with given state handle."""

    def mk_context_state(self, descriptor_handle: str,
                         context_state_handle: str | None = None,
                         adjust_state_version: bool = True,
                         set_associated: bool = False) -> AbstractMultiStateProtocol:
        """Create a new ContextStateContainer."""


class StateTransactionManagerProtocol(AbstractTransactionManagerProtocol):
    """Interface of a TransactionManager that modifies states (except context states)."""

    def actual_descriptor(self, descriptor_handle: str) -> AbstractDescriptorProtocol:
        """Look for new or updated descriptor in current transaction and in mdib."""

    def has_state(self, descriptor_handle: str) -> bool:
        """Check if transaction has a state with given handle."""

    def get_state_transaction_item(self, handle: str) -> TransactionItemProtocol | None:
        """If transaction has a state with given handle, return the transaction-item, otherwise None."""

    def add_state(self, state_container: AbstractStateProtocol, adjust_state_version: bool = True):
        """Add a new state to mdib."""

    def unget_state(self, state_container: AbstractStateProtocol):
        """Forget a state that was provided before by a get_state or add_state call."""

    def get_state(self, descriptor_handle: str) -> AbstractStateProtocol:
        """Read a state from mdib and add it to the transaction."""


class ContextStateTransactionManagerProtocol(StateTransactionManagerProtocol):
    """Interface of a TransactionManager that modifies context states."""

    def get_context_state(self, context_state_handle: str) -> AbstractMultiStateProtocol:
        """Read a ContextState from mdib with given state handle."""

    def mk_context_state(self, descriptor_handle: str,
                         context_state_handle: str | None = None,
                         adjust_state_version: bool = True,
                         set_associated: bool = False) -> AbstractMultiStateProtocol:
        """Create a new ContextStateContainer."""

    def disassociate_all(self,
                         context_descriptor_handle: str,
                         ignored_handle: str | None = None) -> list[str]:
        """Disassociate all associated states in mdib for context_descriptor_handle."""


AnyTransactionManagerProtocol = Union[ContextStateTransactionManagerProtocol,
                                      StateTransactionManagerProtocol,
                                      DescriptorTransactionManagerProtocol]

