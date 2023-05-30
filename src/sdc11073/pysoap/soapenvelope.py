from io import BytesIO
from typing import Optional, List

from lxml import etree as etree_

from ..exceptions import ApiUsageError
from ..namespaces import default_ns_helper as ns_hlp
from ..xml_types import xml_structure as struct
from ..xml_types.basetypes import XMLTypeBase, StringEnum, MessageType, ElementWithText

CHECK_NAMESPACES = False  # can be used to enable additional checks for too many namespaces or undefined namespaces


class SoapResponseException(Exception):

    def __init__(self, response_envelope):
        super().__init__()
        self.response_envelope = response_envelope


class ExtendedDocumentInvalid(etree_.DocumentInvalid):
    pass


_LANGUAGE_ATTR = '{http://www.w3.org/XML/1998/namespace}lang'


def _assert_valid_exception_wrapper(schema, content):
    try:
        schema.assertValid(content)
    except etree_.DocumentInvalid:
        # reformat again to produce better error output
        tmp_str = etree_.tostring(content, pretty_print=True)
        tmp = etree_.parse(BytesIO(tmp_str))
        tmp_str = tmp_str.decode('utf-8')
        try:
            schema.assertValid(tmp)
        except etree_.DocumentInvalid as err:
            msg = f'{str(err)}\n{tmp_str}'
            raise ExtendedDocumentInvalid(msg, error_log=err.error_log) from err


class Soap12Envelope:
    """This represents an outgoing soap envelope"""
    __slots__ = ('_header_nodes', '_payload_element', '_nsmap', 'header_info_block')

    def __init__(self, ns_map: Optional[dict] = None):
        self._header_nodes = []
        self._payload_element = None
        if ns_map is None:
            self._nsmap = {}
        else:
            self._nsmap = ns_map
        for prefix in (ns_hlp.S12, ns_hlp.WSA):  # these are always needed
            self._nsmap[prefix.prefix] = prefix.namespace
        self.header_info_block = None

    def add_header_element(self, element: etree_.Element):
        self._header_nodes.append(element)

    def set_header_info_block(self, header_info_block):
        self.header_info_block = header_info_block

    @property
    def payload_element(self):
        return self._payload_element

    @payload_element.setter
    def payload_element(self, element: etree_.Element):
        if self._payload_element is not None:
            raise ApiUsageError('there can be only one body object')
        self._payload_element = element

    @property
    def nsmap(self) -> dict:
        return self._nsmap

    @property
    def header_nodes(self) -> List[etree_.Element]:
        return self._header_nodes


class ReceivedSoapMessage:
    """Represents a received soap envelope"""
    __slots__ = ('msg_node', 'msg_name', 'raw_data', 'header_info_block', '_doc_root', 'header_node', 'body_node')

    def __init__(self, xml_string, doc_root):
        self.raw_data = xml_string
        self._doc_root = doc_root
        self.header_node = self._doc_root.find(ns_hlp.S12.tag('Header'))
        self.body_node = self._doc_root.find(ns_hlp.S12.tag('Body'))
        self.header_info_block = None
        try:
            self.msg_node = self.body_node[0]
            self.msg_name = etree_.QName(self.msg_node.tag)
        except IndexError:  # body has no content, this can happen
            self.msg_node = None
            self.msg_name = None


# the following classes are named exactly like the types in soap schema, which looks weird sometimes.
class faultcodeEnum(StringEnum):
    DATAENC = f'{ns_hlp.S12.prefix}:DataEncodingUnknown'
    MUSTUNSERSTAND = f'{ns_hlp.S12.prefix}:MustUnderstand'
    RECEIVER = f'{ns_hlp.S12.prefix}:Receiver'
    SENDER = f'{ns_hlp.S12.prefix}:Sender'
    VERSION_MM = f'{ns_hlp.S12.prefix}:VersionMismatch'


class reasontext(ElementWithText):
    lang = struct.StringAttributeProperty(ns_hlp.XML.tag('lang'), default_py_value='en-US')
    _props = ['lang']


class faultreason(XMLTypeBase):
    Text = struct.SubElementListProperty(ns_hlp.S12.tag('Text'), value_class=reasontext)
    _props = ['Text']


class subcode(XMLTypeBase):
    Value = struct.NodeTextQNameProperty(ns_hlp.S12.tag('Value'))
    # optional Subcode Element intentionally omitted, it is of type subcode => recursion, bad idea!
    _props = ['Value']


class faultcode(XMLTypeBase):
    Value = struct.NodeEnumTextProperty(ns_hlp.S12.tag('Value'), faultcodeEnum)
    Subcode = struct.SubElementProperty(ns_hlp.S12.tag('Subcode'), value_class=subcode, is_optional=True)
    _props = ['Value', 'Subcode']


class Fault(MessageType):
    NODETYPE = ns_hlp.S12.tag('Fault')
    action = f'{ns_hlp.WSA.namespace}/fault'
    Code = struct.SubElementProperty(ns_hlp.S12.tag('Code'), value_class=faultcode, default_py_value=faultcode())
    Reason = struct.SubElementProperty(ns_hlp.S12.tag('Reason'), value_class=faultreason, default_py_value=faultreason())
    Node = struct.AnyUriTextElement(ns_hlp.S12.tag('Node'), is_optional=True)
    Role = struct.AnyUriTextElement(ns_hlp.S12.tag('Role'), is_optional=True)
    # Schema says Detail is an "any" type. Here it is modelled as a string that becomes the text of the Detail node
    Detail = struct.NodeStringProperty(ns_hlp.S12.tag('Detail'), is_optional=True)
    _props = ['Code', 'Reason', 'Node', 'Role', 'Detail']
    additional_namespaces = [ns_hlp.XML, ns_hlp.WSE]

    def add_reason_text(self, text: str, lang: str = 'en-US'):
        txt = reasontext()
        txt.lang = lang
        txt.text = text
        self.Reason.Text.append(txt)

    def set_sub_code(self, sub_code: etree_.QName):
        self.Code.Subcode = subcode()
        self.Code.Subcode.Value = sub_code
