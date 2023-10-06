from . import xml_structure
from . import xml_structure as cp
from .addressing_types import EndpointReferenceType
from .basetypes import XMLTypeBase, ElementWithText, MessageType
from .dataconverters import DurationConverter
from .dpws_types import DeviceEventingFilterDialectURI
from sdc11073.namespaces import EventingActions
from sdc11073.namespaces import default_ns_helper

wse_tag = default_ns_helper.WSE.tag
xml_tag = default_ns_helper.XML.tag
### classes that correspond to types in eventing standard


class DeliveryType(XMLTypeBase):
    Mode = cp.AnyURIAttributeProperty("Mode")
    # in ws-eventing NotifyTo is not a mandatory element, but in sdc it is always used.
    NotifyTo = cp.SubElementProperty(wse_tag('NotifyTo'),
                                     value_class=EndpointReferenceType,
                                     default_py_value=EndpointReferenceType())
    _props = ('Mode', 'NotifyTo')


class FilterType(ElementWithText):
    Dialect = cp.AnyURIAttributeProperty('Dialect')
    any = xml_structure.AnyEtreeNodeListProperty(None, is_optional=True)  # noqa: A003
    _props = ('Dialect', 'any')


class Subscribe(MessageType):
    NODETYPE = wse_tag('Subscribe')
    action = EventingActions.Subscribe
    EndTo = cp.SubElementProperty(wse_tag('EndTo'),
                                  value_class=EndpointReferenceType,
                                  is_optional=True)
    Delivery = cp.SubElementProperty(wse_tag('Delivery'),
                                     value_class=DeliveryType,
                                     default_py_value=DeliveryType())
    Expires = cp.NodeDurationProperty(wse_tag('Expires'))
    Filter = cp.SubElementProperty(wse_tag('Filter'),
                                   value_class=FilterType,
                                   is_optional=True)
    _props = ('EndTo', 'Delivery', 'Expires', 'Filter')
    additional_namespaces = (default_ns_helper.WSE,)

    def init_end_to(self):
        self.EndTo = EndpointReferenceType()

    def set_filter(self, filter_text, dialect=DeviceEventingFilterDialectURI.ACTION):
        self.Filter = FilterType()
        self.Filter.text = filter_text
        self.Filter.Dialect = dialect


class SubscribeResponse(MessageType):
    NODETYPE = wse_tag('SubscribeResponse')
    action = EventingActions.SubscribeResponse
    SubscriptionManager = cp.SubElementProperty(wse_tag('SubscriptionManager'),
                                                value_class=EndpointReferenceType,
                                                default_py_value=EndpointReferenceType())
    Expires = cp.NodeDurationProperty(wse_tag('Expires'))
    _props = ('SubscriptionManager', 'Expires')
    additional_namespaces = (default_ns_helper.WSE,)


class Unsubscribe(MessageType):
    NODETYPE = wse_tag('Unsubscribe')
    action = EventingActions.Unsubscribe
    additional_namespaces = (default_ns_helper.WSE,)


class UnsubscribeResponse(MessageType):
    NODETYPE = wse_tag('UnsubscribeResponse')
    action = EventingActions.UnsubscribeResponse
    additional_namespaces = (default_ns_helper.WSE,)

    def as_etree_node(self, *args, **kwargs):
        # Unsubscribe has empty body
        return None


class LanguageSpecificStringType(ElementWithText):
    lang = cp.StringAttributeProperty(attribute_name=xml_tag('lang'))
    _props = ('lang',)


class SubscriptionEnd(MessageType):
    NODETYPE = wse_tag('SubscriptionEnd')
    action = EventingActions.SubscriptionEnd
    SubscriptionManager = cp.SubElementProperty(wse_tag('SubscriptionManager'),
                                                value_class=EndpointReferenceType,
                                                default_py_value=EndpointReferenceType())
    Status = cp.NodeStringProperty(wse_tag('Status'))
    Reason = cp.SubElementListProperty(wse_tag('Reason'),
                                       value_class=LanguageSpecificStringType)
    _props = ('SubscriptionManager', 'Status', 'Reason')
    additional_namespaces = (default_ns_helper.WSE,)

    def add_reason(self, text, lang=None):
        tmp = LanguageSpecificStringType()
        tmp.text = text
        tmp.lang = lang
        self.Reason.append(tmp)


class Renew(MessageType):
    NODETYPE = wse_tag('Renew')
    action = EventingActions.Renew
    Expires = cp.NodeDurationProperty(wse_tag('Expires'))
    _props = ('Expires',)
    additional_namespaces = (default_ns_helper.WSE,)


class RenewResponse(MessageType):
    NODETYPE = wse_tag('RenewResponse')
    action = EventingActions.RenewResponse
    Expires = cp.NodeDurationProperty(wse_tag('Expires'))
    _props = ('Expires',)
    additional_namespaces = (default_ns_helper.WSE,)


class GetStatus(MessageType):
    NODETYPE = wse_tag('GetStatus')
    action = EventingActions.GetStatus
    additional_namespaces = (default_ns_helper.WSE,)


class GetStatusResponse(MessageType):
    NODETYPE = wse_tag('GetStatusResponse')
    action = EventingActions.GetStatusResponse
    Expires = cp.NodeDurationProperty(wse_tag('Expires'))
    _props = ('Expires',)
    additional_namespaces = (default_ns_helper.WSE,)
