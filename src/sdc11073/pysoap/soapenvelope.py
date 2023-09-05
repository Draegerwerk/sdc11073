from __future__ import annotations

from enum import Enum
from typing import TYPE_CHECKING

from lxml import etree as etree_

from sdc11073.exceptions import ApiUsageError
from sdc11073.namespaces import default_ns_helper as ns_hlp
from sdc11073.xml_types import xml_structure as struct
from sdc11073.xml_types.basetypes import ElementWithText, MessageType, XMLTypeBase

if TYPE_CHECKING:
    from sdc11073.xml_types.addressing_types import HeaderInformationBlock
    from sdc11073 import xml_utils

CHECK_NAMESPACES = False  # can be used to enable additional checks for too many namespaces or undefined namespaces


class SoapResponseError(Exception):
    """Exception raised when Response could not be processed."""

    def __init__(self, response_envelope: ReceivedSoapMessage):
        super().__init__()
        self.response_envelope = response_envelope


class ExtendedDocumentInvalid(etree_.DocumentInvalid):
    """Exception for invalid document."""


class Soap12Envelope:
    """Soap12Envelope represents an outgoing soap envelope."""

    __slots__ = ('_header_nodes', '_payload_element', '_nsmap', 'header_info_block')

    def __init__(self, ns_map: dict | None = None):
        self._header_nodes = []
        self._payload_element = None
        if ns_map is None:
            self._nsmap = {}
        else:
            self._nsmap = ns_map
        for prefix in (ns_hlp.S12, ns_hlp.WSA):  # these are always needed
            self._nsmap[prefix.prefix] = prefix.namespace
        self.header_info_block = None

    def add_header_element(self, element: xml_utils.LxmlElement):
        """Add element to soap header."""
        self._header_nodes.append(element)

    def set_header_info_block(self, header_info_block: HeaderInformationBlock):
        """header_info_block contains data needed by ws-addressing."""
        self.header_info_block = header_info_block

    @property
    def payload_element(self) -> xml_utils.LxmlElement:
        """Get payload of soap envelope ( child node of Body element)."""
        return self._payload_element

    @payload_element.setter
    def payload_element(self, element: xml_utils.LxmlElement):
        if self._payload_element is not None:
            raise ApiUsageError('there can be only one body object')
        self._payload_element = element

    @property
    def nsmap(self) -> dict:
        """Get the namespace dictionary for this envelope."""
        return self._nsmap

    @property
    def header_nodes(self) -> list[xml_utils.LxmlElement]:
        """Get the list of header nodes."""
        return self._header_nodes


class ReceivedSoapMessage:
    """Represents a received soap envelope."""

    __slots__ = ('msg_node', 'msg_name', 'raw_data', 'header_info_block', '_doc_root', 'header_node', 'body_node')

    def __init__(self, xml_text: bytes, doc_root: xml_utils.LxmlElement):
        self.raw_data = xml_text
        self._doc_root: xml_utils.LxmlElement = doc_root
        self.header_node: xml_utils.LxmlElement = self._doc_root.find(ns_hlp.S12.tag('Header'))
        self.body_node: xml_utils.LxmlElement = self._doc_root.find(ns_hlp.S12.tag('Body'))
        self.header_info_block: HeaderInformationBlock | None = None
        try:
            self.msg_node = self.body_node[0]
            self.msg_name = etree_.QName(self.msg_node.tag)
        except IndexError:  # body has no content, this can happen
            self.msg_node = None
            self.msg_name = None


# the following classes are named exactly like the types in soap_envelope.xsd schema, which looks weird sometimes.
class faultcodeEnum(Enum):  # noqa: N801
    """Fault codes."""

    DATAENC = ns_hlp.S12.tag('DataEncodingUnknown')
    MUSTUNSERSTAND = ns_hlp.S12.tag('MustUnderstand')
    RECEIVER = ns_hlp.S12.tag('Receiver')
    SENDER = ns_hlp.S12.tag('Sender')
    VERSION_MM = ns_hlp.S12.tag('VersionMismatch')


class reasontext(ElementWithText):  # noqa: N801
    """Text with language attribute."""

    lang: str = struct.StringAttributeProperty(ns_hlp.XML.tag('lang'), default_py_value='en-US')
    _props = ('lang',)


class faultreason(XMLTypeBase):  # noqa: N801
    """List of reasontext."""

    Text: list[reasontext] = struct.SubElementListProperty(ns_hlp.S12.tag('Text'), value_class=reasontext)
    _props = ('Text',)


class subcode(XMLTypeBase):  # noqa: N801
    """Value is a QName."""

    Value: etree_.QName = struct.NodeTextQNameProperty(ns_hlp.S12.tag('Value'))
    # optional Subcode Element intentionally omitted, it is of type subcode => recursion, bad idea!
    _props = ('Value',)


class faultcode(XMLTypeBase):  # noqa: N801
    """Code wit subcode."""

    Value = struct.NodeEnumQNameProperty(ns_hlp.S12.tag('Value'), faultcodeEnum)
    Subcode = struct.SubElementProperty(ns_hlp.S12.tag('Subcode'), value_class=subcode, is_optional=True)
    _props = ('Value', 'Subcode')


class Fault(MessageType):
    """A Fault can be returned instead of a soap envelope."""

    NODETYPE = ns_hlp.S12.tag('Fault')
    action = f'{ns_hlp.WSA.namespace}/fault'
    Code = struct.SubElementProperty(ns_hlp.S12.tag('Code'), value_class=faultcode, default_py_value=faultcode())
    Reason = struct.SubElementProperty(ns_hlp.S12.tag('Reason'), value_class=faultreason,
                                       default_py_value=faultreason())
    Node = struct.AnyUriTextElement(ns_hlp.S12.tag('Node'), is_optional=True)
    Role = struct.AnyUriTextElement(ns_hlp.S12.tag('Role'), is_optional=True)
    # Schema says Detail is an "any" type. Here it is modelled as a string that becomes the text of the Detail node
    Detail = struct.NodeStringProperty(ns_hlp.S12.tag('Detail'), is_optional=True)
    _props = ('Code', 'Reason', 'Node', 'Role', 'Detail')
    additional_namespaces = (ns_hlp.XML, ns_hlp.WSE)

    def add_reason_text(self, text: str, lang: str = 'en-US'):
        """Add reason text to list."""
        txt = reasontext()
        txt.lang = lang
        txt.text = text
        self.Reason.Text.append(txt)

    def set_sub_code(self, sub_code: etree_.QName):
        """Set sub code."""
        self.Code.Subcode = subcode()
        self.Code.Subcode.Value = sub_code
