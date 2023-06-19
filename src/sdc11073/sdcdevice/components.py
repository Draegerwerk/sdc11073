from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Callable, Any, TYPE_CHECKING

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
from ..pysoap.msgfactory import MessageFactory
from ..pysoap.msgreader import MessageReader
from ..pysoap.soapclient import SoapClient
from ..pysoap.soapclient_async import SoapClientAsync
from ..roles.product import MinimalProduct

# pylint: disable=cyclic-import
if TYPE_CHECKING:
    from lxml.etree import QName
    from ..xml_types.wsd_types import ScopesType
    from ..pysoap.msgfactory import MessageFactory
    from ..sdcdevice.servicesfactory import HostedServices
    from .sco import AbstractScoOperationsRegistry
    from ..mdib.devicemdib import DeviceMdibContainer
    from .subscriptionmgr_base import SubscriptionManagerProtocol


# pylint: enable=cyclic-import


# Dependency injection: This class defines which component implementations the sdc device will use.
@dataclass()
class SdcDeviceComponents:
    soap_client_class: type[Any] = None
    msg_factory_class: type[MessageFactory] = None
    msg_reader_class: type[MessageReader] = None
    client_msg_reader_class: type[MessageReader] = None  # the corresponding reader for client
    xml_reader_class: type[MessageReader] = None  # needed to read xml based mdib files
    services_factory: Callable[[Any, dict, Any], HostedServices] = None
    operation_cls_getter: Callable[[QName], type] = None
    sco_operations_registry_class: type[AbstractScoOperationsRegistry] = None
    subscriptions_manager_class: dict[str, type[SubscriptionManagerProtocol]] = None
    role_provider_class: type = None
    scopes_factory: Callable[[DeviceMdibContainer], ScopesType] = None
    hosted_services: dict = None

    def merge(self, other):
        def _merge(attr_name):
            other_value = getattr(other, attr_name)
            if other_value:
                setattr(self, attr_name, other_value)

        _merge('msg_factory_class')
        _merge('msg_reader_class')
        _merge('services_factory')
        _merge('operation_cls_getter')
        _merge('sco_operations_registry_class')
        _merge('role_provider_class')
        _merge('scopes_factory')
        if other.hosted_services is not None:
            self.hosted_services = other.hosted_services
        if other.subscriptions_manager_class is not None:
            for key, value in other.subscriptions_manager_class.items():
                self.subscriptions_manager_class[key] = value


default_sdc_device_components_sync = SdcDeviceComponents(
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
    role_provider_class=MinimalProduct,
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
                     'ContainmentTree': [ContainmentTreeService]}
)

# async variant
default_sdc_device_components_async = copy.deepcopy(default_sdc_device_components_sync)
default_sdc_device_components_async.soap_client_class = SoapClientAsync
default_sdc_device_components_async.subscriptions_manager_class = {'StateEvent': SubscriptionsManagerPathAsync,
                                                                   'Set': SubscriptionsManagerPathAsync}

# set default to sync or async variant
default_sdc_device_components = default_sdc_device_components_async
