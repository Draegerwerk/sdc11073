from io import BytesIO
from typing import Optional, List

from lxml import etree as etree_

from ..namespaces import default_ns_helper as ns_hlp
from ..exceptions import ApiUsageError

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
    __slots__ = ('_header_nodes', '_payload_element', '_nsmap', 'address')

    def __init__(self, ns_map: Optional[dict] = None):
        self._header_nodes = []
        self._payload_element = None
        if ns_map is None:
            self._nsmap = {}
        else:
            self._nsmap = ns_map
        for prefix in (ns_hlp.S12, ns_hlp.WSA):  # these are always needed
            self._nsmap[prefix.prefix] = prefix.namespace
        self.address = None

    def add_header_element(self, element: etree_.Element):
        self._header_nodes.append(element)

    def set_address(self, ws_address):
        self.address = ws_address

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
    __slots__ = ('msg_node', 'msg_name', 'raw_data', 'address', '_doc_root', 'header_node', 'body_node')

    def __init__(self, xml_string, doc_root):
        self.raw_data = xml_string
        self._doc_root = doc_root
        self.header_node = self._doc_root.find(ns_hlp.s12Tag('Header'))
        self.body_node = self._doc_root.find(ns_hlp.s12Tag('Body'))
        self.address = None
        try:
            self.msg_node = self.body_node[0]
            self.msg_name = etree_.QName(self.msg_node.tag)
        except IndexError:  # body has no content, this can happen
            self.msg_node = None
            self.msg_name = None


class FaultCodeEnum:
    """
        Soap Fault codes, see https://www.w3.org/TR/soap12-part1/#faultcodes
    """
    VERSION_MM = 'VersionMismatch'
    MUSTUNSERSTAND = 'MustUnderstand'
    DATAENC = 'DataEncodingUnknown'
    SENDER = 'Sender'
    RECEIVER = 'Receiver'

#class FaultCode:

class SoapFault:
    def __init__(self, code: str, reason: str, sub_code: Optional[etree_.QName] = None, details: Optional[str] = None):
        self.code = code
        self.reason = reason
        self.sub_code = sub_code
        self.details = details

    def __repr__(self):
        return (f'{self.__class__.__name__}(code="{self.code}", sub_code="{self.sub_code}", '
                f'reason="{self.reason}", detail="{self.details}")')
