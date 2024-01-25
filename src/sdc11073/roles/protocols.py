from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:

    from sdc11073.mdib import ProviderMdib
    from sdc11073.mdib.descriptorcontainers import AbstractOperationDescriptorProtocol
    from sdc11073.mdib.transactionsprotocol import RtDataMdibUpdateTransaction
    from sdc11073.provider.operations import OperationDefinitionBase
    from sdc11073.provider.sco import AbstractScoOperationsRegistry
    from sdc11073.xml_types.pm_types import ComponentActivation

    from .providerbase import OperationClassGetter
    from .waveformprovider.realtimesamples import AnnotatorProtocol
    from .waveformprovider.waveforms import WaveformGeneratorBase


class ProductProtocol:
    """A Product aggregates multiple role providers.

    The SdcProvider expects this interface.
    """

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
                                operation_cls_getter: OperationClassGetter) -> OperationDefinitionBase | None:
        """Call make_operation_instance of all role providers, until the first returns not None."""
        ...


class WaveformProviderProtocol(Protocol):
    """WaveformProvider is not a role provider of a product, it is separate.

    The SdcProvider expects this interface.
    """

    is_running: bool

    def __init__(self, mdib: ProviderMdib, log_prefix: str):
        ...

    def register_waveform_generator(self, descriptor_handle: str, wf_generator: WaveformGeneratorBase):
        """Add waveform generator to waveform sources."""
        ...

    def add_annotation_generator(self,
                                 coded_value: Any,
                                 trigger_handle: str,
                                 annotated_handles: list[str]) -> AnnotatorProtocol:
        """Add annotator to list of annotators."""
        ...

    def start(self):
        """Start whatever needs to be started in implementation."""
        ...

    def stop(self):
        """Stop whatever needs to be stopped in implementation."""
        ...

    def set_activation_state(self, descriptor_handle: str, component_activation_state: ComponentActivation):
        """Set the activation state of waveform generator and of Metric state in mdib."""

    def update_all_realtime_samples(self, transaction: RtDataMdibUpdateTransaction):
        """Update all realtime sample states that have a waveform generator registered.

        On transaction commit the mdib will call the appropriate send method of the sdc device.
        """
