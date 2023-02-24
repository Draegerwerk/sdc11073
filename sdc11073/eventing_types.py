from .dataconverters import DurationConverter
from .dpws import DeviceEventingFilterDialectURI
from .mdib import containerproperties as cp
from .mex_types import MessageType
from .namespaces import EventingActions
from .namespaces import default_ns_helper
from .pmtypes import PropertyBasedPMType, ElementWithText
from .addressing import EndpointReferenceType


### classes that correspond to types in eventing standard


class DeliveryType(PropertyBasedPMType):
    Mode = cp.AnyURIAttributeProperty("Mode")
    # in ws-eventing NotifyTo is not a mandatory element, but in sdc it is always used.
    NotifyTo = cp.SubElementProperty(default_ns_helper.wseTag('NotifyTo'),
                                     value_class=EndpointReferenceType,
                                     default_py_value=EndpointReferenceType())
    _props = ['Mode', 'NotifyTo']


class FilterType(ElementWithText):
    Dialect = cp.AnyURIAttributeProperty('Dialect')
    _props = ['Dialect']


class Subscribe(MessageType):
    NODETYPE = default_ns_helper.wseTag('Subscribe')
    action = EventingActions.Subscribe
    EndTo = cp.SubElementProperty(default_ns_helper.wseTag('EndTo'),
                                  value_class=EndpointReferenceType,
                                  is_optional=True)
    Delivery = cp.SubElementProperty(default_ns_helper.wseTag('Delivery'),
                                     value_class=DeliveryType,
                                     default_py_value=DeliveryType())
    Expires = cp.NodeTextProperty(default_ns_helper.wseTag('Expires'), value_converter=DurationConverter)
    Filter = cp.SubElementProperty(default_ns_helper.wseTag('Filter'),
                                   value_class=FilterType,
                                   is_optional=True)
    _props = ['EndTo', 'Delivery', 'Expires', 'Filter']

    def init_end_to(self):
        self.EndTo = EndpointReferenceType()

    def set_filter(self, filter_text, dialect=DeviceEventingFilterDialectURI.ACTION):
        self.Filter = FilterType()
        self.Filter.text = filter_text
        self.Filter.Dialect = dialect


class SubscribeResponse(MessageType):
    NODETYPE = default_ns_helper.wseTag('SubscribeResponse')
    action = EventingActions.SubscribeResponse
    SubscriptionManager = cp.SubElementProperty(default_ns_helper.wseTag('SubscriptionManager'),
                                                value_class=EndpointReferenceType,
                                                default_py_value=EndpointReferenceType())
    Expires = cp.NodeTextProperty(default_ns_helper.wseTag('Expires'), value_converter=DurationConverter)
    _props = ['SubscriptionManager', 'Expires']


class Unsubscribe(MessageType):
    NODETYPE = default_ns_helper.wseTag('Unsubscribe')
    action = EventingActions.Unsubscribe


class LanguageSpecificStringType(ElementWithText):
    lang = cp.StringAttributeProperty(attribute_name=default_ns_helper.xmlTag('lang'))
    _props = ['lang']


class SubscriptionEnd(MessageType):
    NODETYPE = default_ns_helper.wseTag('SubscriptionEnd')
    action = EventingActions.SubscriptionEnd
    SubscriptionManager = cp.SubElementProperty(default_ns_helper.wseTag('SubscriptionManager'),
                                                value_class=EndpointReferenceType,
                                                default_py_value=EndpointReferenceType())
    Status = cp.NodeStringProperty(default_ns_helper.wseTag('Status'))
    Reason = cp.SubElementListProperty(default_ns_helper.wseTag('Reason'),
                                       value_class=LanguageSpecificStringType)
    _props = ['SubscriptionManager', 'Status', 'Reason']

    def add_reason(self, text, lang=None):
        tmp = LanguageSpecificStringType()
        tmp.text = text
        tmp.lang = lang
        self.Reason.append(tmp)


class Renew(MessageType):
    NODETYPE = default_ns_helper.wseTag('Renew')
    action = EventingActions.Renew
    Expires = cp.NodeTextProperty(default_ns_helper.wseTag('Expires'), value_converter=DurationConverter)
    _props = ['Expires']


class RenewResponse(MessageType):
    NODETYPE = default_ns_helper.wseTag('RenewResponse')
    action = EventingActions.RenewResponse
    Expires = cp.NodeTextProperty(default_ns_helper.wseTag('Expires'), value_converter=DurationConverter)
    _props = ['Expires']


class GetStatus(MessageType):
    NODETYPE = default_ns_helper.wseTag('GetStatus')
    action = EventingActions.GetStatus


class GetStatusResponse(MessageType):
    NODETYPE = default_ns_helper.wseTag('GetStatusResponse')
    action = EventingActions.GetStatusResponse
    Expires = cp.NodeTextProperty(default_ns_helper.wseTag('Expires'), value_converter=DurationConverter)
    _props = ['Expires']
