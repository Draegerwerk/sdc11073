"""Declare protocols for a Product and a WaveformProvider."""

from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
    from sdc11073.mdib.providermdibprotocol import ProviderMdibProtocol
    from sdc11073.provider.operations import OperationDefinitionBase
    from sdc11073.provider.protocols.roleproviderprotocol import OperationClassGetter
    from sdc11073.provider.sco import AbstractScoOperationsRegistry


class ProductProtocol(Protocol):
    """A Product aggregates multiple role providers. It is associated to a single sco.

    The SdcProvider expects this interface.
    """

    def __init__(self, mdib: ProviderMdibProtocol, sco: AbstractScoOperationsRegistry, log_prefix: str | None = None):
        """Create a product."""
        ...

    def init_operations(self):
        """Register all actively provided operations."""
        ...

    def stop(self):
        """Stop all role providers."""
        ...

    def make_operation_instance(
        self,
        operation_descriptor_container: AbstractOperationDescriptorProtocol,
        operation_cls_getter: OperationClassGetter,
    ) -> OperationDefinitionBase | None:
        """Call make_operation_instance of all role providers, until the first returns not None."""
        ...
