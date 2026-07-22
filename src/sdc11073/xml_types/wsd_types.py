"""WS-Discovery XML types (Hello, Bye, Probe, Resolve and their matches)."""

from sdc11073.namespaces import default_ns_helper
from sdc11073.xml_types import xml_structure as struct
from sdc11073.xml_types.addressing_types import EndpointReferenceType
from sdc11073.xml_types.basetypes import ElementWithTextList, MessageType, XMLTypeBase

########## Meta Data Exchange #########
wsa_tag = default_ns_helper.WSA.tag  # shortcut
wsd_tag = default_ns_helper.WSD.tag  # shortcut


class QNameListType(struct.NodeTextQNameListProperty):
    """Property representing a whitespace-separated list of QNames."""


class ScopesType(ElementWithTextList):
    """WS-Discovery ScopesType: a URI list with an optional MatchBy attribute."""

    # text is a URI list
    MatchBy = struct.AnyURIAttributeProperty('MatchBy')
    _props = ('MatchBy',)

    def __init__(self, value: str | None = None, match_by: str | None = None):
        super().__init__()
        if value is not None:
            self.text.append(value)
        self.MatchBy = match_by


class HelloType(MessageType):
    """WS-Discovery Hello message type."""

    NODETYPE = wsd_tag('Hello')
    action = f'{default_ns_helper.WSD.namespace}/Hello'
    EndpointReference = struct.SubElementProperty(
        wsa_tag('EndpointReference'), value_class=EndpointReferenceType, default_py_value=EndpointReferenceType()
    )
    Types = QNameListType(wsd_tag('Types'), is_optional=True)
    Scopes = struct.SubElementProperty(wsd_tag('Scopes'), value_class=ScopesType, is_optional=True)
    XAddrs = struct.NodeTextListProperty(wsd_tag('XAddrs'), value_class=str, is_optional=True)
    MetadataVersion = struct.NodeIntProperty(wsd_tag('MetadataVersion'), default_py_value=1)
    _props = ('EndpointReference', 'Types', 'Scopes', 'XAddrs', 'MetadataVersion')


class ByeType(MessageType):
    """WS-Discovery Bye message type."""

    NODETYPE = wsd_tag('Bye')
    action = f'{default_ns_helper.WSD.namespace}/Bye'
    EndpointReference = struct.SubElementProperty(
        wsa_tag('EndpointReference'), value_class=EndpointReferenceType, default_py_value=EndpointReferenceType()
    )
    Types = QNameListType(wsd_tag('Types'), is_optional=True)
    Scopes = struct.SubElementProperty(wsd_tag('Scopes'), value_class=ScopesType, is_optional=True)
    XAddrs = struct.NodeTextListProperty(wsd_tag('XAddrs'), value_class=str, is_optional=True)
    MetadataVersion = struct.NodeIntProperty(wsd_tag('MetadataVersion'), is_optional=True)
    _props = ('EndpointReference', 'Types', 'Scopes', 'XAddrs', 'MetadataVersion')


class ProbeType(MessageType):
    """WS-Discovery Probe message type."""

    NODETYPE = wsd_tag('Probe')
    action = f'{default_ns_helper.WSD.namespace}/Probe'
    Types = QNameListType(wsd_tag('Types'), is_optional=True)
    Scopes = struct.SubElementProperty(wsd_tag('Scopes'), value_class=ScopesType, is_optional=True)
    _props = ('Types', 'Scopes')


class ProbeMatchType(XMLTypeBase):
    """WS-Discovery ProbeMatch type describing a single matching target service."""

    EndpointReference = struct.SubElementProperty(
        wsa_tag('EndpointReference'), value_class=EndpointReferenceType, default_py_value=EndpointReferenceType()
    )
    Types = QNameListType(wsd_tag('Types'), is_optional=True)
    Scopes = struct.SubElementProperty(wsd_tag('Scopes'), value_class=ScopesType, is_optional=True)
    XAddrs = struct.NodeTextListProperty(wsd_tag('XAddrs'), value_class=str, is_optional=True)
    MetadataVersion = struct.NodeIntProperty(wsd_tag('MetadataVersion'), default_py_value=1)
    _props = ('EndpointReference', 'Types', 'Scopes', 'XAddrs', 'MetadataVersion')


class ProbeMatchesType(MessageType):
    """WS-Discovery ProbeMatches message type carrying a list of ProbeMatch entries."""

    NODETYPE = wsd_tag('ProbeMatches')
    action = f'{default_ns_helper.WSD.namespace}/ProbeMatches'
    ProbeMatch = struct.SubElementListProperty(wsd_tag('ProbeMatch'), value_class=ProbeMatchType)
    _props = ('ProbeMatch',)
    additional_namespaces = (default_ns_helper.WSD, default_ns_helper.WSA)


class ResolveType(MessageType):
    """WS-Discovery Resolve message type."""

    NODETYPE = wsd_tag('Resolve')
    action = f'{default_ns_helper.WSD.namespace}/Resolve'
    EndpointReference = struct.SubElementProperty(
        wsa_tag('EndpointReference'), value_class=EndpointReferenceType, default_py_value=EndpointReferenceType()
    )
    _props = ('EndpointReference',)


class ResolveMatchType(XMLTypeBase):
    """WS-Discovery ResolveMatch type describing the resolved target service."""

    EndpointReference = struct.SubElementProperty(
        wsa_tag('EndpointReference'), value_class=EndpointReferenceType, default_py_value=EndpointReferenceType()
    )
    Types = QNameListType(wsd_tag('Types'), is_optional=True)
    Scopes = struct.SubElementProperty(wsd_tag('Scopes'), value_class=ScopesType, is_optional=True)
    XAddrs = struct.NodeTextListProperty(wsd_tag('XAddrs'), value_class=str, is_optional=True)
    MetadataVersion = struct.NodeIntProperty(wsd_tag('MetadataVersion'), default_py_value=1)
    _props = ('EndpointReference', 'Types', 'Scopes', 'XAddrs', 'MetadataVersion')


class ResolveMatchesType(MessageType):
    """WS-Discovery ResolveMatches message type."""

    NODETYPE = wsd_tag('ResolveMatches')
    action = f'{default_ns_helper.WSD.namespace}/ResolveMatches'
    ResolveMatch = struct.SubElementProperty(wsd_tag('ResolveMatch'), value_class=ResolveMatchType, is_optional=True)
    _props = ('ResolveMatch',)


class AppSequenceType(XMLTypeBase):
    """WS-Discovery AppSequence type used in the SOAP header to order messages."""

    NODETYPE = wsd_tag('AppSequence')
    # used in soap header
    InstanceId = struct.IntegerAttributeProperty('InstanceId', is_optional=False)
    SequenceId = struct.AnyURIAttributeProperty('SequenceId')
    MessageNumber = struct.IntegerAttributeProperty('MessageNumber', is_optional=False)
    _props = ('InstanceId', 'SequenceId', 'MessageNumber')
