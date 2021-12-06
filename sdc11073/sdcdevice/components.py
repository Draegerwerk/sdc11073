from __future__ import annotations

from dataclasses import dataclass
from typing import Type, Callable, List, Any, TYPE_CHECKING


from ..pysoap.soapclient import SoapClient
from ..pysoap.msgfactory import MessageFactoryDevice
from ..pysoap.msgreader import MessageReaderDevice
from ..roles.product import MinimalProduct

from .sdc_handlers import mk_scopes, mk_all_services
from .sco import get_operation_class, ScoOperationsRegistry
from .subscriptionmgr import SubscriptionsManagerPath
from .hostedserviceimpl import by_msg_tag
from .services.waveformserviceimpl import WaveformService
from .services.descriptioneventserviceimpl import  DescriptionEventService
from .services.contextserviceimpl import ContextService
from .services.getserviceimpl import GetService
from .services.setserviceimpl import SetService
from .services.containmenttreeserviceimpl import ContainmentTreeService
from .services.stateeventserviceimpl import StateEventService
from .services.localizationservice import LocalizationService


# pylint: disable=cyclic-import
if TYPE_CHECKING:
    from lxml.etree import QName
    from ..wsdiscovery import Scope
    from ..pysoap.msgfactory import MessageFactory
    from ..pysoap.msgreader import MessageReader
    from ..sdcdevice.sdc_handlers import HostedServices
    from .sco import AbstractScoOperationsRegistry
    from .subscriptionmgr import SubscriptionsManagerBase
    from ..mdib.devicemdib import DeviceMdibContainer
    from ..httprequesthandler import RequestData
# pylint: enable=cyclic-import


# Dependency injection: This class defines which component implementations the sdc device will use.
@dataclass()
class SdcDeviceComponents:
    soap_client_class: Type[Any] = None
    msg_factory_class: Type[MessageFactory] = None
    msg_reader_class: Type[MessageReader] = None
    xml_reader_class: Type[MessageReader] = None  # needed to read xml based mdib files
    services_factory: Callable[[Any, dict, Any], HostedServices] = None
    operation_cls_getter: Callable[[QName], type] = None
    sco_operations_registry_class: Type[AbstractScoOperationsRegistry] = None
    subscriptions_manager_class: Type[SubscriptionsManagerBase] = None
    role_provider_class: type = None
    scopes_factory: Callable[[DeviceMdibContainer], List[Scope]] = None
    msg_dispatch_method: Callable[[RequestData], str] = None
    service_handlers: dict = None

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
        _merge('subscriptions_manager_class')
        _merge('role_provider_class')
        _merge('scopes_factory')
        if other.service_handlers:
            for key, value in other.service_handlers.items():
                self.service_handlers[key] = value


default_sdc_device_components = SdcDeviceComponents(
    soap_client_class = SoapClient,
    msg_factory_class=MessageFactoryDevice,
    msg_reader_class=MessageReaderDevice,
    xml_reader_class=MessageReaderDevice,
    services_factory=mk_all_services,
    operation_cls_getter=get_operation_class,
    sco_operations_registry_class=ScoOperationsRegistry,
    subscriptions_manager_class=SubscriptionsManagerPath,
    role_provider_class=MinimalProduct,
    scopes_factory=mk_scopes,
    msg_dispatch_method=by_msg_tag,
    service_handlers={'ContainmentTreeService': ContainmentTreeService,
                      'GetService': GetService,
                      'StateEventService': StateEventService,
                      'ContextService': ContextService,
                      'WaveformService': WaveformService,
                      'SetService': SetService,
                      'DescriptionEventService': DescriptionEventService,
                      'LocalizationService': LocalizationService}
)
