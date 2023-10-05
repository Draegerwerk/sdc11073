from . import xml_structure as struct
from .basetypes import XMLTypeBase, MessageType, ElementWithTextList
from .addressing_types import EndpointReferenceType
from sdc11073.namespaces import default_ns_helper
from typing import Optional

########## Meta Data Exchange #########
wsa_tag = default_ns_helper.WSA.tag  # shortcut
wsd_tag = default_ns_helper.WSD.tag  # shortcut

class QNameListType(struct.NodeTextQNameListProperty):
    pass

class ScopesType(ElementWithTextList):
    # text is a URI list
    MatchBy = struct.AnyURIAttributeProperty('MatchBy')
    _props = ('MatchBy',)

    def __init__(self, value: Optional[str] = None, match_by: Optional[str] = None):
        super().__init__()
        if value is not None:
            self.text.append(value)
        self.MatchBy = match_by


class HelloType(MessageType):
    NODETYPE = wsd_tag('Hello')
    action = f'{default_ns_helper.WSD.namespace}/Hello'
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
    _props = ('EndpointReference', 'Types', 'Scopes', 'XAddrs', 'MetadataVersion')


class ByeType(MessageType):
    NODETYPE = wsd_tag('Bye')
    action = f'{default_ns_helper.WSD.namespace}/Bye'
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
    _props = ('EndpointReference', 'Types', 'Scopes', 'XAddrs', 'MetadataVersion')


class ProbeType(MessageType):
    NODETYPE = wsd_tag('Probe')
    action = f'{default_ns_helper.WSD.namespace}/Probe'
    Types  = QNameListType(wsd_tag('Types'), is_optional=True)
    Scopes  = struct.SubElementProperty(wsd_tag('Scopes'),
                                        value_class=ScopesType,
                                        is_optional=True)
    _props = ('Types', 'Scopes')


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
    _props = ('EndpointReference', 'Types', 'Scopes', 'XAddrs', 'MetadataVersion')


class ProbeMatchesType(MessageType):
    NODETYPE = wsd_tag('ProbeMatches')
    action = f'{default_ns_helper.WSD.namespace}/ProbeMatches'
    ProbeMatch = struct.SubElementListProperty(wsd_tag('ProbeMatch'),
                                               value_class=ProbeMatchType)
    _props = ('ProbeMatch',)
    additional_namespaces = (default_ns_helper.WSD, default_ns_helper.WSA)


class ResolveType(MessageType):
    NODETYPE = wsd_tag('Resolve')
    action = f'{default_ns_helper.WSD.namespace}/Resolve'
    EndpointReference = struct.SubElementProperty(wsa_tag('EndpointReference'),
                                                  value_class=EndpointReferenceType,
                                                  default_py_value=EndpointReferenceType())
    _props = ('EndpointReference',)


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
    _props = ('EndpointReference', 'Types', 'Scopes', 'XAddrs', 'MetadataVersion')


class ResolveMatchesType(MessageType):
    NODETYPE = wsd_tag('ResolveMatches')
    action = f'{default_ns_helper.WSD.namespace}/ResolveMatches'
    ResolveMatch = struct.SubElementProperty(wsd_tag('ResolveMatch'),
                                               value_class=ResolveMatchType,
                                             is_optional=True)
    _props = ('ResolveMatch',)


class AppSequenceType(XMLTypeBase):
    NODETYPE = wsd_tag('AppSequence')
    # used in soap header
    InstanceId = struct.IntegerAttributeProperty('InstanceId', is_optional=False)
    SequenceId = struct.AnyURIAttributeProperty('SequenceId')
    MessageNumber = struct.IntegerAttributeProperty('MessageNumber', is_optional=False)
    _props = ('InstanceId', 'SequenceId', 'MessageNumber')
