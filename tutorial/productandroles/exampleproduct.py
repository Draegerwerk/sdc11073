"""Implementation of products.

A product is a set of role providers that handle operations and other tasks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from sdc11073.provider import RoleProviderComponents
from sdc11073.provider.baseproduct import BaseProduct
from tutorial.productandroles.alarmprovider import (
    AlertDelegateProvider,
    AlertPreCommitHandler,
    AlertSystemStateMaintainer,
)
from tutorial.productandroles.audiopauseprovider import AudioPauseProvider
from tutorial.productandroles.clockprovider import GenericSDCClockProvider
from tutorial.productandroles.componentprovider import GenericSetComponentStateOperationProvider
from tutorial.productandroles.contextprovider import (
    EnsembleContextProvider,
    GenericPatientContextProvider,
    LocationContextProvider,
)
from tutorial.productandroles.metricprovider import GenericMetricProvider
from tutorial.productandroles.operationprovider import OperationProvider
from tutorial.productandroles.waveformprovider.waveformproviderimpl import GenericWaveformProvider

if TYPE_CHECKING:
    from sdc11073.mdib.providermdibprotocol import ProviderMdibProtocol
    from sdc11073.provider.sco import AbstractScoOperationsRegistry


class ExampleProduct(BaseProduct):
    """An example product including multiple role provider."""

    def __init__(self, mdib: ProviderMdibProtocol, sco: AbstractScoOperationsRegistry, log_prefix: str | None = None):
        super().__init__(mdib, sco, log_prefix)
        self.metric_provider = GenericMetricProvider(mdib, log_prefix=log_prefix)  # needed in a test
        self._ordered_role_providers.extend(
            [
                AudioPauseProvider(mdib, log_prefix=log_prefix),
                GenericSDCClockProvider(mdib, log_prefix=log_prefix),
                GenericPatientContextProvider(mdib, log_prefix=log_prefix),
                AlertDelegateProvider(mdib, log_prefix=log_prefix),
                AlertSystemStateMaintainer(mdib, log_prefix=log_prefix),
                AlertPreCommitHandler(mdib, log_prefix=log_prefix),
                self.metric_provider,
                OperationProvider(mdib, log_prefix=log_prefix),
                GenericSetComponentStateOperationProvider(mdib, log_prefix=log_prefix),
            ],
        )


class ExtendedExampleProduct(ExampleProduct):
    """Add EnsembleContextProvider and LocationContextProvider."""

    def __init__(self, mdib: ProviderMdibProtocol, sco: AbstractScoOperationsRegistry, log_prefix: str | None = None):
        super().__init__(mdib, sco, log_prefix)
        self._ordered_role_providers.extend(
            [
                EnsembleContextProvider(mdib, log_prefix=log_prefix),
                LocationContextProvider(mdib, log_prefix=log_prefix),
            ],
        )


EXAMPLE_ROLE_PROVIDER_COMPONENTS = RoleProviderComponents(
    role_provider_class=ExampleProduct,
    waveform_provider_class=GenericWaveformProvider,
)
