"""Python classes that correspond to the types defined in the WS-Eventing standard."""

from __future__ import annotations

import enum

from sdc11073.namespaces import EventingActions, default_ns_helper
from sdc11073.xml_types import xml_structure
from sdc11073.xml_types import xml_structure as cp
from sdc11073.xml_types.addressing_types import EndpointReferenceType
from sdc11073.xml_types.basetypes import ElementWithText, MessageType, XMLTypeBase
from sdc11073.xml_types.dpws_types import DeviceEventingFilterDialectURI

wse_tag = default_ns_helper.WSE.tag
xml_tag = default_ns_helper.XML.tag
### classes that correspond to types in eventing standard


class DeliveryType(XMLTypeBase):
    """Describes how and where notifications shall be delivered."""

    Mode = cp.AnyURIAttributeProperty('Mode')
    # in ws-eventing NotifyTo is not a mandatory element, but in sdc it is always used.
    NotifyTo = cp.SubElementProperty(
        wse_tag('NotifyTo'), value_class=EndpointReferenceType, default_py_value=EndpointReferenceType()
    )
    _props = ('Mode', 'NotifyTo')


class FilterType(ElementWithText):
    """Restricts the notifications a subscription delivers to those matching the filter."""

    Dialect = cp.AnyURIAttributeProperty('Dialect')
    any = xml_structure.AnyEtreeNodeListProperty(None, is_optional=True)
    _props = ('Dialect', 'any')


class Subscribe(MessageType):
    """Request to create a subscription."""

    NODETYPE = wse_tag('Subscribe')
    action = EventingActions.Subscribe
    EndTo = cp.SubElementProperty(wse_tag('EndTo'), value_class=EndpointReferenceType, is_optional=True)
    Delivery = cp.SubElementProperty(wse_tag('Delivery'), value_class=DeliveryType, default_py_value=DeliveryType())
    Expires = cp.NodeDurationProperty(wse_tag('Expires'))
    Filter = cp.SubElementProperty(wse_tag('Filter'), value_class=FilterType, is_optional=True)
    _props = ('EndTo', 'Delivery', 'Expires', 'Filter')
    additional_namespaces = (default_ns_helper.WSE,)

    def init_end_to(self):
        """Initialize the EndTo endpoint reference with an empty instance."""
        self.EndTo = EndpointReferenceType()

    def set_filter(self, filter_text: str, dialect: str = DeviceEventingFilterDialectURI.ACTION):
        """Set the subscription filter and its dialect."""
        self.Filter = FilterType()
        self.Filter.text = filter_text
        self.Filter.Dialect = dialect


class SubscribeResponse(MessageType):
    """Response to a Subscribe request, containing the subscription manager reference."""

    NODETYPE = wse_tag('SubscribeResponse')
    action = EventingActions.SubscribeResponse
    SubscriptionManager = cp.SubElementProperty(
        wse_tag('SubscriptionManager'), value_class=EndpointReferenceType, default_py_value=EndpointReferenceType()
    )
    Expires = cp.NodeDurationProperty(wse_tag('Expires'))
    _props = ('SubscriptionManager', 'Expires')
    additional_namespaces = (default_ns_helper.WSE,)


class Unsubscribe(MessageType):
    """Request to cancel an existing subscription."""

    NODETYPE = wse_tag('Unsubscribe')
    action = EventingActions.Unsubscribe
    additional_namespaces = (default_ns_helper.WSE,)


class UnsubscribeResponse(MessageType):
    """Response to an Unsubscribe request."""

    NODETYPE = wse_tag('UnsubscribeResponse')
    action = EventingActions.UnsubscribeResponse
    additional_namespaces = (default_ns_helper.WSE,)

    def as_etree_node(self, *args: object, **kwargs: object) -> None:
        """Return None because UnsubscribeResponse has an empty body."""


class LanguageSpecificStringType(ElementWithText):
    """A text value together with the language it is expressed in."""

    lang = cp.StringAttributeProperty(attribute_name=xml_tag('lang'))
    _props = ('lang',)


class SubscriptionEndStatus(str, enum.Enum):
    """WS-Eventing subscription end status codes as full URIs (see WS-Eventing section 3.5).

    These are the values enumerated by the eventing standard's SubscriptionEndCodeType.
    """

    DELIVERY_FAILURE = f'{default_ns_helper.WSE.namespace}/DeliveryFailure'
    SOURCE_SHUTTING_DOWN = f'{default_ns_helper.WSE.namespace}/SourceShuttingDown'
    SOURCE_CANCELLING = f'{default_ns_helper.WSE.namespace}/SourceCancelling'


class SubscriptionEnd(MessageType):
    """Notification sent by the event source when a subscription ends."""

    NODETYPE = wse_tag('SubscriptionEnd')
    action = EventingActions.SubscriptionEnd
    SubscriptionManager = cp.SubElementProperty(
        wse_tag('SubscriptionManager'), value_class=EndpointReferenceType, default_py_value=EndpointReferenceType()
    )
    Status = cp.NodeStringProperty(wse_tag('Status'))
    Reason = cp.SubElementListProperty(wse_tag('Reason'), value_class=LanguageSpecificStringType)
    _props = ('SubscriptionManager', 'Status', 'Reason')
    additional_namespaces = (default_ns_helper.WSE,)

    def add_reason(self, text: str, lang: str | None = None):
        """Append a human-readable reason for the subscription end."""
        tmp = LanguageSpecificStringType()
        tmp.text = text
        tmp.lang = lang
        self.Reason.append(tmp)


class Renew(MessageType):
    """Request to extend the lifetime of a subscription."""

    NODETYPE = wse_tag('Renew')
    action = EventingActions.Renew
    Expires = cp.NodeDurationProperty(wse_tag('Expires'))
    _props = ('Expires',)
    additional_namespaces = (default_ns_helper.WSE,)


class RenewResponse(MessageType):
    """Response to a Renew request, containing the updated expiration."""

    NODETYPE = wse_tag('RenewResponse')
    action = EventingActions.RenewResponse
    Expires = cp.NodeDurationProperty(wse_tag('Expires'))
    _props = ('Expires',)
    additional_namespaces = (default_ns_helper.WSE,)


class GetStatus(MessageType):
    """Request the current status of a subscription."""

    NODETYPE = wse_tag('GetStatus')
    action = EventingActions.GetStatus
    additional_namespaces = (default_ns_helper.WSE,)


class GetStatusResponse(MessageType):
    """Response to a GetStatus request, containing the current expiration."""

    NODETYPE = wse_tag('GetStatusResponse')
    action = EventingActions.GetStatusResponse
    Expires = cp.NodeDurationProperty(wse_tag('Expires'))
    _props = ('Expires',)
    additional_namespaces = (default_ns_helper.WSE,)
