from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Callable

from sdc11073.pysoap.msgfactory import MessageFactory
from sdc11073.pysoap.msgreader import MessageReader
from sdc11073.pysoap.soapclient import SoapClient
from sdc11073.pysoap.soapclient_async import SoapClientAsync
from sdc11073.roles.product import DefaultProduct
from sdc11073.roles.waveformprovider.waveformproviderimpl import GenericWaveformProvider

from .operations import get_operation_class
from .porttypes.containmenttreeserviceimpl import ContainmentTreeService
from .porttypes.contextserviceimpl import ContextService
from .porttypes.descriptioneventserviceimpl import DescriptionEventService
from .porttypes.getserviceimpl import GetService
from .porttypes.localizationservice import LocalizationService
from .porttypes.setserviceimpl import SetService
from .porttypes.stateeventserviceimpl import StateEventService
from .porttypes.waveformserviceimpl import WaveformService
from .sco import ScoOperationsRegistry
from .scopesfactory import mk_scopes
from .servicesfactory import mk_all_services
from .subscriptionmgr import PathDispatchingSubscriptionsManager
from .subscriptionmgr_async import SubscriptionsManagerPathAsync

# pylint: disable=cyclic-import
if TYPE_CHECKING:
    from lxml.etree import QName

    from sdc11073 import provider
    from sdc11073.mdib.providermdib import ProviderMdib
    from sdc11073.provider.servicesfactory import HostedServices
    from sdc11073.xml_types.wsd_types import ScopesType
    from sdc11073.namespaces import PrefixNamespace

    from .sco import AbstractScoOperationsRegistry
    from .subscriptionmgr_base import SubscriptionManagerProtocol


# pylint: enable=cyclic-import


# Dependency injection: This class defines which component implementations the sdc device will use.
@dataclass()
class SdcProviderComponents:
    """Dependency injection: This class defines which component implementations the sdc provider will use."""

    soap_client_class: type[Any] = None
    msg_factory_class: type[MessageFactory] = None
    msg_reader_class: type[MessageReader] = None
    client_msg_reader_class: type[MessageReader] = None  # the corresponding reader for client
    xml_reader_class: type[MessageReader] = None  # needed to read xml based mdib files
    services_factory: Callable[[provider.SdcProvider, SdcProviderComponents, dict], HostedServices] = None
    operation_cls_getter: Callable[[QName], type] = None
    sco_operations_registry_class: type[AbstractScoOperationsRegistry] = None
    subscriptions_manager_class: dict[str, type[SubscriptionManagerProtocol]] = None
    role_provider_class: type = None
    waveform_provider_class: type | None = None
    scopes_factory: Callable[[ProviderMdib], ScopesType] = None
    hosted_services: dict = None
    additional_schema_specs: list[PrefixNamespace] = field(default_factory=list)

    def merge(self, other: SdcProviderComponents):
        """Add data from other to self."""

        def _merge(attr_name: str):
            other_value = getattr(other, attr_name)
            if other_value:
                setattr(self, attr_name, other_value)

        _merge('msg_factory_class')
        _merge('msg_reader_class')
        _merge('services_factory')
        _merge('operation_cls_getter')
        _merge('sco_operations_registry_class')
        _merge('role_provider_class')
        _merge('waveform_provider_class')
        _merge('scopes_factory')
        if other.hosted_services is not None:
            self.hosted_services = other.hosted_services
        if other.subscriptions_manager_class is not None:
            for key, value in other.subscriptions_manager_class.items():
                self.subscriptions_manager_class[key] = value
        self.additional_schema_specs = list(set(self.additional_schema_specs).union(set(other.additional_schema_specs)))


default_sdc_provider_components_sync = SdcProviderComponents(
    soap_client_class=SoapClient,
    msg_factory_class=MessageFactory,
    msg_reader_class=MessageReader,
    client_msg_reader_class=MessageReader,
    xml_reader_class=MessageReader,
    services_factory=mk_all_services,
    operation_cls_getter=get_operation_class,
    sco_operations_registry_class=ScoOperationsRegistry,
    subscriptions_manager_class={'StateEvent': PathDispatchingSubscriptionsManager,
                                 'Set': PathDispatchingSubscriptionsManager},
    role_provider_class=DefaultProduct,
    waveform_provider_class=GenericWaveformProvider,
    scopes_factory=mk_scopes,
    # this defines the structure of the services: keys are the names of the dpws hosts,
    # value is a list of port type implementation classes
    hosted_services={'Get': [GetService,
                             LocalizationService],
                     'StateEvent': [StateEventService,
                                    ContextService,
                                    DescriptionEventService,
                                    WaveformService],
                     'Set': [SetService],
                     'ContainmentTree': [ContainmentTreeService]},
)

# async variant
default_sdc_provider_components_async = copy.deepcopy(default_sdc_provider_components_sync)
default_sdc_provider_components_async.soap_client_class = SoapClientAsync
default_sdc_provider_components_async.subscriptions_manager_class = {'StateEvent': SubscriptionsManagerPathAsync,
                                                                   'Set': SubscriptionsManagerPathAsync}

# set default to sync or async variant
default_sdc_provider_components = default_sdc_provider_components_async
