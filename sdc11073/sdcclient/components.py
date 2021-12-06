from __future__ import annotations

from dataclasses import dataclass
from typing import Type, Any, TYPE_CHECKING

from .httpserver import SOAPNotificationsHandler, NotificationsReceiver
from .notificationsdispatcher import NotificationsDispatcherByBody
from .operations import OperationsManager
from .serviceclients.containmenttreeservice import CTreeServiceClient
from .serviceclients.contextservice import ContextServiceClient
from .serviceclients.descriptioneventservice import DescriptionEventClient
from .serviceclients.getservice import GetServiceClient
from .serviceclients.localizationservice import LocalizationServiceClient
from .serviceclients.setservice import SetServiceClient
from .serviceclients.stateeventservice import StateEventClient
from .serviceclients.waveformservice import WaveformClient
from .subscription import ClientSubscriptionManager
from ..pysoap.msgfactory import MessageFactoryClient
from ..pysoap.msgreader import MessageReaderClient
from ..pysoap.soapclient import SoapClient

# pylint: disable=cyclic-import
if TYPE_CHECKING:
    from ..pysoap.msgfactory import MessageFactory
    from ..pysoap.msgreader import MessageReader


# pylint: enable=cyclic-import


# Dependency injection: This class defines which component implementations the sdc client will use.
@dataclass()
class SdcClientComponents:
    soap_client_class: Type[Any] = None
    msg_factory_class: Type[MessageFactory] = None
    msg_reader_class: Type[MessageReader] = None
    notifications_receiver_class: type = None
    notifications_handler_class: type = None
    notifications_dispatcher_class: type = None
    subscription_manager_class: type = None
    operations_manager_class: type = None
    service_handlers: dict = None

    def merge(self, other):
        def _merge(attrname):
            other_value = getattr(other, attrname)
            if other_value:
                setattr(self, attrname, other_value)

        _merge('msg_factory_class')
        _merge('msg_reader_class')
        _merge('notifications_receiver_class')
        _merge('notifications_handler_class')
        _merge('subscription_manager_class')
        _merge('operations_manager_class')
        if other.service_handlers:
            for key, value in other.service_handlers.items():
                self.service_handlers[key] = value


default_sdc_client_components = SdcClientComponents(
    soap_client_class=SoapClient,
    msg_factory_class=MessageFactoryClient,
    msg_reader_class=MessageReaderClient,
    notifications_receiver_class=NotificationsReceiver,
    notifications_handler_class=SOAPNotificationsHandler,
    notifications_dispatcher_class=NotificationsDispatcherByBody,
    subscription_manager_class=ClientSubscriptionManager,
    operations_manager_class=OperationsManager,
    service_handlers={'ContainmentTreeService': CTreeServiceClient,
                      'GetService': GetServiceClient,
                      'StateEventService': StateEventClient,
                      'ContextService': ContextServiceClient,
                      'WaveformService': WaveformClient,
                      'SetService': SetServiceClient,
                      'DescriptionEventService': DescriptionEventClient,
                      'LocalizationService': LocalizationServiceClient,
                      }
)
