from io import BytesIO

from lxml import etree as etree_

from sdc11073.namespaces import s12Tag, nsmap

CHECK_NAMESPACES = False  # can be used to enable additional checks for too many namespaces or undefined namespaces


class SoapResponseException(Exception):

    def __init__(self, response_envelope):
        super().__init__()
        self.response_envelope = response_envelope


class ExtendedDocumentInvalid(etree_.DocumentInvalid):
    pass


def _get_text(node, id_string, namespace_map):
    if node is None:
        return None
    tmp = node.find(id_string, namespace_map)
    if tmp is None:
        return None
    return tmp.text


class _GenericNode:
    def __init__(self, node):
        self._node = node

    def as_etree_subnode(self, root_node):
        root_node.append(self._node)


_LANGUAGE_ATTR = '{http://www.w3.org/XML/1998/namespace}lang'


def _assert_valid_exception_wrapper(schema, content):
    try:
        schema.assertValid(content)
    except etree_.DocumentInvalid:
        # reformat and validate again to produce better error output
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
    __slots__ = ('_header_objects', '_body_object', '_doc_root', '_nsmap', 'address')

    def __init__(self, ns_map):
        self._header_objects = []
        self._body_object = None
        self._doc_root = None
        self._nsmap = ns_map
        self.address = None

    def add_header_object(self, obj):
        assert hasattr(obj, 'as_etree_subnode')
        self._header_objects.append(obj)
        self._doc_root = None

    # def add_header_element(self, element):
    #     self.add_header_object(_GenericNode(element))
    #     self._doc_root = None

    def add_reference_parameter(self, element):
        self.add_header_object(_GenericNode(element))
        self._doc_root = None

    def _add_body_object(self, obj):
        if self._body_object is not None:
            raise RuntimeError('there can be only one body object')
        assert hasattr(obj, 'as_etree_subnode')
        self._body_object = obj
        self._doc_root = None

    def add_body_element(self, element):
        self._add_body_object(_GenericNode(element))
        self._doc_root = None

    def set_address(self, ws_address):
        self.address = ws_address

    @property
    def body_node(self):
        body = etree_.Element(s12Tag('Body'), nsmap=self._nsmap)
        if self._body_object:
            self._body_object.as_etree_subnode(body)
        return body

    @property
    def nsmap(self):
        return self._nsmap

    @property
    def header_objects(self):
        return self._header_objects

    @property
    def body_object(self):
        return self._body_object

    def validate_body(self, schema):
        if schema is None:
            return
        body = etree_.Element(s12Tag('Body'), nsmap=self._nsmap)
        self._body_object.as_etree_subnode(body)
        _assert_valid_exception_wrapper(schema, body[0])


class ReceivedSoapMessage:
    """Represents a received soap envelope"""
    __slots__ = ('msg_node', 'msg_name', 'raw_data', 'address', '_doc_root', 'header_node', 'body_node')

    def __init__(self, xml_string, doc_root):
        self.raw_data = xml_string
        self._doc_root = doc_root
        self.header_node = self._doc_root.find('s12:Header', nsmap)
        self.body_node = self._doc_root.find('s12:Body', nsmap)
        self.address = None  # WsAddress.from_etree_node(self.header_node)
        try:
            self.msg_node = self.body_node[0]
            self.msg_name = etree_.QName(self.msg_node.tag)
        except IndexError:  # body has no content, this can happen
            self.msg_node = None
            self.msg_name = None

    def validate_body(self, schema):
        if schema is None:
            return
        _assert_valid_exception_wrapper(schema, self.msg_node)


class SoapFault:
    def __init__(self, code, reason, subcode=None, details=None):
        self.code = code
        self.reason = reason
        self.subcode = subcode
        self.details = details

    def __repr__(self):
        return (f'{self.__class__.__name__}(code="{self.code}", subcode="{self.subcode}", '
                f'reason="{self.reason}", detail="{self.details}")')


class SoapFaultCode:
    """
        Soap Fault codes, see https://www.w3.org/TR/soap12-part1/#faultcodes
    """
    VERSION_MM = 'VersionMismatch'
    MUSTUNSERSTAND = 'MustUnderstand'
    DATAENC = 'DataEncodingUnknown'
    SENDER = 'Sender'
    RECEIVER = 'Receiver'
