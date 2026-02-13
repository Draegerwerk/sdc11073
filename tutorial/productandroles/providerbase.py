"""The module implements the base class of role providers."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sdc11073 import loghelper
from sdc11073.provider.protocols.roleproviderprotocol import RoleProviderProtocol

if TYPE_CHECKING:
    from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
    from sdc11073.mdib.providermdibprotocol import ProviderMdibProtocol
    from sdc11073.mdib.transactionsprotocol import AbstractTransactionManagerProtocol
    from sdc11073.provider.operations import ExecuteHandler, OperationDefinitionBase, TimeoutHandler
    from sdc11073.provider.protocols.roleproviderprotocol import OperationClassGetter
    from sdc11073.provider.sco import AbstractScoOperationsRegistry


class RoleProvider(RoleProviderProtocol):
    """Base class for all role implementations."""

    def __init__(self, mdib: ProviderMdibProtocol, log_prefix: str):
        self._mdib = mdib
        self._logger = loghelper.get_logger_adapter(f'sdc.device.{self.__class__.__name__}', log_prefix)

    def stop(self):
        """Stop whatever needs to be stopped.

        Implement method in derived class if needed.
        """

    def init_operations(self, sco: AbstractScoOperationsRegistry):
        """Initialize and start whatever the provider needs.

        Implement method in derived class if needed.
        """

    def make_operation_instance(
        self,
        operation_descriptor_container: AbstractOperationDescriptorProtocol,  # noqa: ARG002
        operation_cls_getter: OperationClassGetter,  # noqa: ARG002
    ) -> OperationDefinitionBase | None:
        """Return an operation definition instance for this operation or None.

        If a mdib already has operations defined, this method can connect a handler to a given operation descriptor.
        Use case: initialization from an existing mdib.
        """
        return None

    def make_missing_operations(
        self,
        sco: AbstractScoOperationsRegistry,  # noqa: ARG002
    ) -> list[OperationDefinitionBase]:
        """Create operations that this role provider needs.

        This method is called after all existing operations from mdib have been registered.
        If a role provider needs to add operations beyond that, it can do it here.
        """
        return []

    def on_pre_commit(self, mdib: ProviderMdibProtocol, transaction: AbstractTransactionManagerProtocol):
        """Manipulate the transaction if needed.

        Derived classes can overwrite this method.
        """

    def on_post_commit(self, mdib: ProviderMdibProtocol, transaction: AbstractTransactionManagerProtocol):
        """Run stuff after transaction.

        Derived classes can overwrite this method.
        """

    @staticmethod
    def _mk_operation_from_operation_descriptor(
        operation_descriptor_container: AbstractOperationDescriptorProtocol,
        operation_cls_getter: OperationClassGetter,
        operation_handler: ExecuteHandler,
        timeout_handler: TimeoutHandler | None = None,
    ) -> OperationDefinitionBase:
        """Create an OperationDefinition for the operation_descriptor_container."""
        op_cls = operation_cls_getter(operation_descriptor_container.NODETYPE)
        return op_cls(
            operation_descriptor_container.Handle,
            operation_descriptor_container.OperationTarget,
            operation_handler=operation_handler,
            timeout_handler=timeout_handler,
            coded_value=operation_descriptor_container.Type,
        )
