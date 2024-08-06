from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from sdc11073.pysoap.msgfactory import MessageFactory
from sdc11073.pysoap.msgreader import MessageReader
from sdc11073.pysoap.soapclient import SoapClient

from .operations import OperationsManager, OperationsManagerProtocol
from .request_handler_deferred import DispatchKeyRegistryDeferred
from .serviceclients.containmenttreeservice import CTreeServiceClient
from .serviceclients.contextservice import ContextServiceClient
from .serviceclients.descriptioneventservice import DescriptionEventClient
from .serviceclients.getservice import GetServiceClient
from .serviceclients.localizationservice import LocalizationServiceClient
from .serviceclients.setservice import SetServiceClient
from .serviceclients.stateeventservice import StateEventClient
from .serviceclients.waveformservice import WaveformClient
from .subscription import ConsumerSubscriptionManager, ConsumerSubscriptionManagerProtocol

if TYPE_CHECKING:
    from sdc11073.pysoap.soapclient import SoapClientProtocol
    from sdc11073.dispatch.dispatchkey import RequestDispatcherProtocol
    from sdc11073.namespaces import PrefixNamespace


@dataclass()
class SdcConsumerComponents:
    """Dependency injection: This class defines which component implementations the sdc consumer will use."""

    soap_client_class: type[SoapClientProtocol] = None
    msg_factory_class: type[MessageFactory] = None
    msg_reader_class: type[MessageReader] = None
    action_dispatcher_class: type[RequestDispatcherProtocol] = None
    subscription_manager_class: type[ConsumerSubscriptionManagerProtocol] = None
    operations_manager_class: type[OperationsManagerProtocol] | None = None
    service_handlers: list = None
    additional_schema_specs: list[PrefixNamespace] = field(default_factory=list)

    def merge(self, other: SdcConsumerComponents):
        """Add data from other to self."""
        def _merge(attr_name: str):
            other_value = getattr(other, attr_name)
            if other_value:
                setattr(self, attr_name, other_value)

        _merge('msg_factory_class')
        _merge('msg_reader_class')
        _merge('action_dispatcher_class')
        _merge('subscription_manager_class')
        _merge('operations_manager_class')
        if other.service_handlers:
            # append handlers that are not yet present
            for handler in other.service_handlers:
                if handler not in self.service_handlers:
                    self.service_handlers.append(handler)
        self.additional_schema_specs = list(set(self.additional_schema_specs).union(set(other.additional_schema_specs)))


default_sdc_consumer_components = SdcConsumerComponents(
    soap_client_class=SoapClient,
    msg_factory_class=MessageFactory,
    msg_reader_class=MessageReader,
    action_dispatcher_class=DispatchKeyRegistryDeferred,  # defaults to deferred handling
    subscription_manager_class=ConsumerSubscriptionManager,
    operations_manager_class=OperationsManager,
    service_handlers=[CTreeServiceClient,
                      GetServiceClient,
                      StateEventClient,
                      ContextServiceClient,
                      WaveformClient,
                      SetServiceClient,
                      DescriptionEventClient,
                      LocalizationServiceClient,
                      ],
)
