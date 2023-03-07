from . import xml_structure as struct
from .basetypes import XMLTypeBase, MessageType, ElementWithTextList
from .addressing_types import EndpointReferenceType
from ..namespaces import default_ns_helper

########## Meta Data Exchange #########
wsa_tag = default_ns_helper.wsaTag  # shortcut
wsd_tag = default_ns_helper.wsdTag  # shortcut

class QNameListType(struct.NodeTextQNameListProperty):
    pass

class ScopesType(ElementWithTextList):
    MatchBy = struct.AnyURIAttributeProperty('MatchBy')
    _props = ['MatchBy']


class HelloType(XMLTypeBase):
    EndpointReference = struct.SubElementProperty(wsa_tag('EndpointReference'),
                                                  value_class=EndpointReferenceType,
                                                  default_py_value=EndpointReferenceType())
    Types  = QNameListType(wsd_tag('Types'), is_optional=True)
    Scopes  = struct.SubElementProperty(wsd_tag('Scopes'),
                                        value_class=ScopesType,
                                        is_optional=True)
    XAddrs  = struct.NodeTextListProperty(wsd_tag('XAddrs'),
                                          value_class=str,
                                          is_optional=True)
    MetadataVersion = struct.NodeIntProperty(wsd_tag('MetadataVersion'), default_py_value=1)
    _props = ['EndpointReference', 'Types', 'Scopes', 'XAddrs', 'MetadataVersion']


class ByeType(XMLTypeBase):
    EndpointReference = struct.SubElementProperty(wsa_tag('EndpointReference'),
                                                  value_class=EndpointReferenceType,
                                                  default_py_value=EndpointReferenceType())
    Types  = QNameListType(wsd_tag('Types'), is_optional=True)
    Scopes  = struct.SubElementProperty(wsd_tag('Scopes'),
                                        value_class=ScopesType,
                                        is_optional=True)
    XAddrs  = struct.NodeTextListProperty(wsd_tag('XAddrs'),
                                          value_class=str,
                                          is_optional=True)
    MetadataVersion = struct.NodeIntProperty(wsd_tag('MetadataVersion'), is_optional=True)
    _props = ['EndpointReference', 'Types', 'Scopes', 'XAddrs', 'MetadataVersion']


class ProbeType(MessageType):
    NODETYPE = wsd_tag('Probe')
    action = f'{default_ns_helper.WSD.namespace}/Probe'
    Types  = QNameListType(wsd_tag('Types'), is_optional=True)
    Scopes  = struct.SubElementProperty(wsd_tag('Scopes'),
                                        value_class=ScopesType,
                                        is_optional=True)
    _props = ['Types', 'Scopes']


class ProbeMatchType(XMLTypeBase):
    EndpointReference = struct.SubElementProperty(wsa_tag('EndpointReference'),
                                                  value_class=EndpointReferenceType,
                                                  default_py_value=EndpointReferenceType())
    Types  = QNameListType(wsd_tag('Types'), is_optional=True)
    Scopes  = struct.SubElementProperty(wsd_tag('Scopes'),
                                        value_class=ScopesType,
                                        is_optional=True)
    XAddrs  = struct.NodeTextListProperty(wsd_tag('XAddrs'),
                                          value_class=str,
                                          is_optional=True)
    MetadataVersion = struct.NodeIntProperty(wsd_tag('MetadataVersion'), default_py_value=1)
    _props = ['EndpointReference', 'Types', 'Scopes', 'XAddrs', 'MetadataVersion']


class ProbeMatchesType(MessageType):
    NODETYPE = wsd_tag('ProbeMatches')
    action = f'{default_ns_helper.WSD.namespace}/ProbeMatches'
    ProbeMatch = struct.SubElementListProperty(wsd_tag('ProbeMatch'),
                                               value_class=ProbeMatchType)
    _props = ['ProbeMatch']
    additional_namespaces = [default_ns_helper.WSD, default_ns_helper.WSA]


class ResolveType(XMLTypeBase):
    EndpointReference = struct.SubElementProperty(wsa_tag('EndpointReference'),
                                                  value_class=EndpointReferenceType,
                                                  default_py_value=EndpointReferenceType())
    _props = ['EndpointReference']


class ResolveMatchType(XMLTypeBase):
    EndpointReference = struct.SubElementProperty(wsa_tag('EndpointReference'),
                                                  value_class=EndpointReferenceType,
                                                  default_py_value=EndpointReferenceType())
    Types  = QNameListType(wsd_tag('Types'), is_optional=True)
    Scopes  = struct.SubElementProperty(wsd_tag('Scopes'),
                                        value_class=ScopesType,
                                        is_optional=True)
    XAddrs  = struct.NodeTextListProperty(wsd_tag('XAddrs'),
                                          value_class=str,
                                          is_optional=True)
    MetadataVersion = struct.NodeIntProperty(wsd_tag('MetadataVersion'), default_py_value=1)
    _props = ['EndpointReference', 'Types', 'Scopes', 'XAddrs', 'MetadataVersion']


class ResolveMatchesType(XMLTypeBase):
    ResolveMatch = struct.SubElementProperty(wsd_tag('ResolveMatch'),
                                               value_class=ResolveMatchType,
                                             is_optional=True)
    _props = ['ResolveMatch']
