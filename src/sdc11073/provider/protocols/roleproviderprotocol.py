"""Protocol for RoleProvider implementations."""

from collections.abc import Callable
from typing import Protocol

from lxml import etree

from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
from sdc11073.mdib.providermdibprotocol import ProviderMdibProtocol
from sdc11073.mdib.transactionsprotocol import AnyTransactionManagerProtocol
from sdc11073.provider.operations import OperationDefinitionBase
from sdc11073.provider.sco import AbstractScoOperationsRegistry

OperationClassGetter = Callable[[etree.QName], type[OperationDefinitionBase]]


class RoleProviderProtocol(Protocol):
    """A RoleProvider implements operation handlers and can run other jobs that the role requires.

    This Interface is expected by BaseProduct.
    """

    def stop(self):
        """Stop worker threads etc."""
        ...

    def init_operations(self, sco: AbstractScoOperationsRegistry):
        """Init instance.

        Method is called on start.
        """
        ...

    def make_operation_instance(
        self,
        operation_descriptor_container: AbstractOperationDescriptorProtocol,
        operation_cls_getter: OperationClassGetter,
    ) -> OperationDefinitionBase | None:
        """Return a callable for this operation or None.

        If a mdib already has operations defined, this method can connect a handler to a given operation descriptor.
        Use case: initialization from an existing mdib
        """
        ...

    def make_missing_operations(self, sco: AbstractScoOperationsRegistry) -> list[OperationDefinitionBase]:
        """Make_missing_operations is called after all existing operations from mdib have been registered.

        If a role provider needs to add operations beyond that, it can do it here.
        """

    def on_pre_commit(self, mdib: ProviderMdibProtocol, transaction: AnyTransactionManagerProtocol):
        """Manipulate operation (e.g. add more states)."""
        ...

    def on_post_commit(self, mdib: ProviderMdibProtocol, transaction: AnyTransactionManagerProtocol):
        """Implement actions after the transaction."""
        ...
