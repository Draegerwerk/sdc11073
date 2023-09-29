from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:

    from sdc11073.mdib import ProviderMdib
    from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
    from sdc11073.mdib.transactions import RtDataMdibUpdateTransaction, TransactionManagerProtocol
    from sdc11073.provider.operations import OperationDefinitionBase
    from sdc11073.provider.sco import AbstractScoOperationsRegistry
    from sdc11073.xml_types.pm_types import ComponentActivation

    from .providerbase import OperationClassGetter
    from .waveformprovider.realtimesamples import AnnotatorProtocol
    from .waveformprovider.waveforms import WaveformGeneratorBase


class ProviderRoleProtocol(Protocol):
    """A ProviderRole implements operation handlers and can run other jobs that the role requires."""

    def __init__(self, mdib: ProviderMdib, log_prefix: str):
        ...

    def stop(self):
        """Stop worker threads etc."""
        ...

    def init_operations(self, sco: AbstractScoOperationsRegistry):
        """Init instance.

        Method is called on start.
        """
        ...

    def make_operation_instance(self,
                                operation_descriptor_container: AbstractOperationDescriptorProtocol,
                                operation_cls_getter: OperationClassGetter) -> type[OperationDefinitionBase] | None:
        """Return a callable for this operation or None.

        If a mdib already has operations defined, this method can connect a handler to a given operation descriptor.
        Use case: initialization from an existing mdib
        """
        ...

    def make_missing_operations(self, sco: AbstractScoOperationsRegistry) -> list[OperationDefinitionBase]:
        """Make_missing_operations is called after all existing operations from mdib have been registered.

        If a role provider needs to add operations beyond that, it can do it here.
        """
        ...

    def on_pre_commit(self, mdib: ProviderMdib, transaction: TransactionManagerProtocol):
        """Manipulate operation (e.g. add more states)."""
        ...

    def on_post_commit(self, mdib: ProviderMdib, transaction: TransactionManagerProtocol):
        """Implement actions after the transaction."""
        ...


class ProductProtocol:
    """A Product aggregates multiple role providers."""

    def __init__(self,
                 mdib: ProviderMdib,
                 sco: AbstractScoOperationsRegistry,
                 log_prefix: str | None = None):
        """Create a product."""
        ...

    def init_operations(self):
        """Register all actively provided operations."""
        ...

    def stop(self):
        """Stop all role providers."""
        ...

    def make_operation_instance(self,
                                operation_descriptor_container: AbstractOperationDescriptorProtocol,
                                operation_cls_getter: OperationClassGetter) -> type[OperationDefinitionBase] | None:
        """Call make_operation_instance of all role providers, until the first returns not None."""
        ...


class WaveformProviderProtocol(Protocol):
    """WaveformProvider is not a role provider of a product, it is separate."""

    is_running: bool

    def __init__(self, mdib: ProviderMdib, log_prefix: str):
        ...

    def register_waveform_generator(self, descriptor_handle: str, wf_generator: WaveformGeneratorBase):
        ...

    def add_annotation_generator(self,
                                 coded_value: Any,
                                 trigger_handle: str,
                                 annotated_handles: list[str]) -> AnnotatorProtocol:
        ...

    def start(self):
        ...

    def stop(self):
        ...

    def set_activation_state(self, descriptor_handle: str, component_activation_state: ComponentActivation):
        """Set the activation state of waveform generator and of Metric state in mdib."""

    def update_all_realtime_samples(self, transaction: RtDataMdibUpdateTransaction):
        """Update all realtime sample states that have a waveform generator registered.

        On transaction commit the mdib will call the appropriate send method of the sdc device.
        """
