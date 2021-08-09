import copy
import uuid
from io import BytesIO

from lxml import etree as etree_

from sdc11073.namespaces import Prefixes
from sdc11073.namespaces import wsaTag, wseTag, dpwsTag, s12Tag, xmlTag, nsmap, WSA_ANONYMOUS, docname_from_qname
from .. import isoduration

CHECK_NAMESPACES = False  # can be used to enable additional checks for too many namespaces or undefined namespaces

DIALECT_ACTION = '{}/Action'.format(Prefixes.DPWS.namespace)
DIALECT_THIS_MODEL = '{}/ThisModel'.format(Prefixes.DPWS.namespace)
DIALECT_THIS_DEVICE = '{}/ThisDevice'.format(Prefixes.DPWS.namespace)
DIALECT_RELATIONSHIP = '{}/Relationship'.format(Prefixes.DPWS.namespace)
HOST_TYPE = '{}/host'.format(Prefixes.DPWS.namespace)


class SoapResponseException(Exception):

    def __init__(self, response_envelope):
        super().__init__()
        self.response_envelope = response_envelope


class ExtendedDocumentInvalid(etree_.DocumentInvalid):
    pass


# def merge_dicts(*args):
#     result = {}
#     for d in args:
#         for k, v in d.items():
#             if not k in result:
#                 result[k] = v
#             else:
#                 if result[k] != v:
#                     raise RuntimeError('Merge Conflict key={}, value1={}, value2={}'.format(k, result[k], v))
#     return result


def _get_text(node, id_string, namespace_map):
    if node is None:
        return None
    tmp = node.find(id_string, namespace_map)
    if tmp is None:
        return None
    return tmp.text


class GenericNode:
    def __init__(self, node):
        self._node = node

    def as_etree_subnode(self, root_node):
        root_node.append(self._node)


class WsaEndpointReferenceType:
    ''' Acc. to "http://www.w3.org/2005/08/addressing"

    '''
    __slots__ = ('address', 'reference_parameters_node', 'metadata_node')

    def __init__(self, address, reference_parameters_node=None, metadata_node=None):
        self.address = address  # type="wsa:AttributedURI", which is an xs:anyURI element
        self.reference_parameters_node = None
        self.metadata_node = None
        if reference_parameters_node is not None:
            if hasattr(reference_parameters_node, 'tag') and reference_parameters_node.tag == wsaTag(
                    'ReferenceParameters'):
                self.reference_parameters_node = reference_parameters_node  # any content allowed. optional
            else:
                self.reference_parameters_node = etree_.Element(wsaTag('ReferenceParameters'))
                self.reference_parameters_node.extend(reference_parameters_node)
        if metadata_node is not None:
            if hasattr(metadata_node, 'tag') and metadata_node.tag == wsaTag('MetaData'):
                self.metadata_node = metadata_node  # any content allowed. optional
            else:
                self.metadata_node = etree_.Element(wsaTag('MetaData'))
                self.metadata_node.extend(metadata_node)

    def __str__(self):
        return 'WsaEndpointReferenceType: address={}'.format(self.address)

    @classmethod
    def from_etree_node(cls, root_node):
        address_node = root_node.find('wsa:Address', nsmap)
        address = address_node.text
        reference_parameters_node = root_node.find('wsa:ReferenceParameters', nsmap)
        metadata_node = root_node.find('wsa:MetaData', nsmap)
        ret = cls(address, reference_parameters_node, metadata_node)
        return ret

    def as_etree_subnode(self, root_node):
        node = etree_.SubElement(root_node, wsaTag('Address'))
        node.text = self.address
        if self.reference_parameters_node is not None:
            root_node.append(copy.copy(self.reference_parameters_node))
        if self.metadata_node is not None:
            root_node.append(self.metadata_node)


class WsAddress:
    __slots__ = ('message_id', 'addr_to', 'addr_from', 'reply_to', 'fault_to', 'action',
                 'relates_to', 'reference_parameters_node', 'relationship_type')

    def __init__(self, action, message_id=None, addr_to=None, relates_to=None, addr_from=None, reply_to=None,
                 fault_to=None, reference_parameters_node=None,
                 relationship_type=None):  # pylint: disable=too-many-arguments
        '''

        :param action: xs:anyURI string, required
        :param message_id: xs:anyURI string or None or False; default is None
                          if None, a message_id is generated automatically
                          if False, no message ID is generated ( makes only sense for testing )
        :param addr_to: xs:anyURI string, optional
        :param relates_to: xs:anyURI string, 0...n
        :param addr_from: WsaEndpointReferenceType instance, optional
        :param reply_to: WsaEndpointReferenceType instance, optional
        :param faultTo: WsaEndpointReferenceType instance, optional
        :param reference_parameters_node: any node, optional
        :param relationship_type: a QName, optional
        '''
        self.action = action
        if message_id == False:
            self.message_id = None
        else:
            self.message_id = message_id or uuid.uuid4().urn
        self.addr_to = addr_to
        self.relates_to = relates_to
        self.addr_from = addr_from
        self.reply_to = reply_to
        self.fault_to = fault_to
        self.reference_parameters_node = reference_parameters_node
        self.relationship_type = relationship_type

    def mk_reply_address(self, action):
        return WsAddress(action=action, relates_to=self.message_id)

    def as_etree_subnode(self, root_node):
        # To (OPTIONAL), defaults to anonymous
        node = etree_.SubElement(root_node, wsaTag('To'), attrib={s12Tag('mustUnderstand'): 'true'})
        node.text = self.addr_to or WSA_ANONYMOUS
        # From
        if self.addr_from:
            self.addr_from.as_etree_subnode(root_node)
        # ReplyTo (OPTIONAL), defaults to anonymous
        if self.reply_to:
            self.reply_to.as_etree_subnode(root_node)
        # FaultTo (OPTIONAL)
        if self.fault_to:
            self.fault_to.as_etree_subnode(root_node)
        # Action (REQUIRED)
        node = etree_.SubElement(root_node, wsaTag('Action'), attrib={s12Tag('mustUnderstand'): 'true'})
        node.text = self.action
        # MessageID (OPTIONAL)
        if self.message_id:
            node = etree_.SubElement(root_node, wsaTag('MessageID'))
            node.text = self.message_id
        # RelatesTo (OPTIONAL)
        if self.relates_to:
            node = etree_.SubElement(root_node, wsaTag('RelatesTo'))
            node.text = self.relates_to
            if self.relationship_type is not None:
                node.set('RelationshipType', self.relationship_type)
        if self.reference_parameters_node:
            root_node.append(copy.copy(self.reference_parameters_node))

    @classmethod
    def from_etree_node(cls, root_node):
        message_id = _get_text(root_node, 'wsa:MessageID', nsmap)
        addr_to = _get_text(root_node, 'wsa:To', nsmap)
        action = _get_text(root_node, 'wsa:Action', nsmap)
        relates_to = _get_text(root_node, 'wsa:RelatesTo', nsmap)
        relationship_type = None
        relates_to_node = root_node.find('wsa:RelatesTo', nsmap)
        if relates_to_node is not None:
            relates_to = relates_to_node.text
            relationshiptype_text = relates_to_node.attrib.get('RelationshipType')
            if relationshiptype_text:
                # split into namespace, localname
                namespace, localname = relationshiptype_text.rsplit('/', 1)
                relationship_type = etree_.QName(namespace, localname)

        def mk_endpoint_reference(id_string):
            tmp = root_node.find(id_string, nsmap)
            if tmp is None:
                return None
            return WsaEndpointReferenceType.from_etree_node(tmp)

        addr_from = mk_endpoint_reference('wsa:From')
        reply_to = mk_endpoint_reference('wsa:ReplyTo')
        fault_to = mk_endpoint_reference('wsa:FaultTo')
        reference_parameters_node = root_node.find('wsa:ReferenceParameters', nsmap)

        return cls(message_id=message_id,
                   addr_to=addr_to,
                   action=action,
                   relates_to=relates_to,
                   addr_from=addr_from,
                   reply_to=reply_to,
                   fault_to=fault_to,
                   reference_parameters_node=reference_parameters_node,
                   relationship_type=relationship_type)


_LANGUAGE_ATTR = '{http://www.w3.org/XML/1998/namespace}lang'


class WsSubscribe:
    MODE_PUSH = '{}/DeliveryModes/Push'.format(Prefixes.WSE.namespace)
    __slots__ = ('delivery_mode', 'notify_to', 'end_to', 'expires', 'filter')

    def __init__(self, notify_to,
                 expires,
                 end_to=None,
                 filter_=None,
                 delivery_mode=None):
        '''
        @param notify_to: a WsaEndpointReferenceType
        @param expires: duration in seconds ( absolute date not supported)
        @param end_to: a WsaEndpointReferenceType or None
        @param delivery_mode: defaults to self.MODE_PUSH
        '''
        self.delivery_mode = delivery_mode or self.MODE_PUSH
        self.notify_to = notify_to
        self.end_to = end_to
        self.expires = expires
        self.filter = filter_

    def as_etree_subnode(self, root_node):
        # To (OPTIONAL), defaults to anonymous
        subscribe = etree_.SubElement(root_node, wseTag('Subscribe'),
                                      nsmap=Prefixes.partial_map(Prefixes.WSE, Prefixes.WSA))
        if self.end_to is not None:
            end_to_node = etree_.SubElement(subscribe, wseTag('EndTo'))
            self.end_to.as_etree_subnode(end_to_node)
        delivery = etree_.SubElement(subscribe, wseTag('Delivery'))
        delivery.set('Mode', self.delivery_mode)

        notify_to_node = etree_.SubElement(delivery, wseTag('NotifyTo'))
        self.notify_to.as_etree_subnode(notify_to_node)

        exp = etree_.SubElement(subscribe, wseTag('Expires'))
        exp.text = isoduration.duration_string(self.expires)
        fil = etree_.SubElement(subscribe, wseTag('Filter'))
        fil.set('Dialect', DIALECT_ACTION)  # Is this always this string?
        fil.text = self.filter

    @classmethod
    def from_etree_node(cls, root_node):
        raise NotImplementedError  # pylint: disable=unused-argument


class DPWSThisDevice:
    __slots__ = ('friendly_name', 'firmware_version', 'serial_number')

    def __init__(self, friendly_name, firmware_version, serial_number):
        if isinstance(friendly_name, dict):
            self.friendly_name = friendly_name
        else:
            self.friendly_name = {'': friendly_name}  # localized texts
        self.firmware_version = firmware_version
        self.serial_number = serial_number

    @classmethod
    def from_etree_node(cls, root_node):
        friendly_name = {}  # localized texts
        for f_name in root_node.findall('dpws:FriendlyName', nsmap):
            friendly_name[f_name.get(_LANGUAGE_ATTR)] = f_name.text
        firmware_version = _get_text(root_node, 'dpws:FirmwareVersion', nsmap)
        serial_number = _get_text(root_node, 'dpws:SerialNumber', nsmap)
        return cls(friendly_name, firmware_version, serial_number)

    def as_etree_subnode(self, root_node):
        this_device = etree_.SubElement(root_node, dpwsTag('ThisDevice'), nsmap=Prefixes.partial_map(Prefixes.DPWS))
        for lang, name in self.friendly_name.items():
            friendly_name = etree_.SubElement(this_device, dpwsTag('FriendlyName'))
            friendly_name.text = name
            friendly_name.set(_LANGUAGE_ATTR, lang)
        firmware_version = etree_.SubElement(this_device, dpwsTag('FirmwareVersion'))
        firmware_version.text = self.firmware_version
        serial_number = etree_.SubElement(this_device, dpwsTag('SerialNumber'))
        serial_number.text = self.serial_number

    def __str__(self):
        return 'DPWSThisDevice: friendly_name={}, firmware_version="{}", serial_number="{}"'.format(self.friendly_name,
                                                                                                    self.firmware_version,
                                                                                                    self.serial_number)


class DPWSThisModel:
    __slots__ = ('manufacturer', 'manufacturer_url', 'model_name', 'model_number', 'model_url', 'presentation_url')

    def __init__(self, manufacturer, manufacturer_url, model_name, model_number, model_url, presentation_url):
        if isinstance(manufacturer, dict):
            self.manufacturer = manufacturer
        else:
            self.manufacturer = {None: manufacturer}  # localized texts
        self.manufacturer_url = manufacturer_url
        if isinstance(model_name, dict):
            self.model_name = model_name
        else:
            self.model_name = {None: model_name}  # localized texts
        self.model_number = model_number
        self.model_url = model_url
        self.presentation_url = presentation_url

    def __str__(self):
        return 'DPWSThisModel: manufacturer={}, model_name="{}", model_number="{}"'.format(self.manufacturer,
                                                                                           self.model_name,
                                                                                           self.model_number)

    @classmethod
    def from_etree_node(cls, root_node):
        manufacturer = {}  # localized texts
        for manufact_node in root_node.findall('dpws:Manufacturer', nsmap):
            manufacturer[manufact_node.get(_LANGUAGE_ATTR)] = manufact_node.text
        manufacturer_url = _get_text(root_node, 'dpws:ManufacturerUrl', nsmap)
        model_name = {}  # localized texts
        for model_name_node in root_node.findall('dpws:ModelName', nsmap):
            model_name[model_name_node.get(_LANGUAGE_ATTR)] = model_name_node.text
        model_number = _get_text(root_node, 'dpws:ModelNumber', nsmap)
        model_url = _get_text(root_node, 'dpws:ModelUrl', nsmap)
        presentation_url = _get_text(root_node, 'dpws:PresentationUrl', nsmap)
        return cls(manufacturer, manufacturer_url, model_name, model_number, model_url, presentation_url)

    def as_etree_subnode(self, root_node):
        this_model = etree_.SubElement(root_node, dpwsTag('ThisModel'), nsmap=Prefixes.partial_map(Prefixes.DPWS))
        for lang, name in self.manufacturer.items():
            manufacturer = etree_.SubElement(this_model, dpwsTag('Manufacturer'))
            manufacturer.text = name
            if lang is not None:
                manufacturer.set(_LANGUAGE_ATTR, lang)

        manufacturer_url = etree_.SubElement(this_model, dpwsTag('ManufacturerUrl'))
        manufacturer_url.text = self.manufacturer_url

        for lang, name in self.model_name.items():
            manufacturer = etree_.SubElement(this_model, dpwsTag('ModelName'))
            manufacturer.text = name
            if lang is not None:
                manufacturer.set(_LANGUAGE_ATTR, lang)

        model_number = etree_.SubElement(this_model, dpwsTag('ModelNumber'))
        model_number.text = self.model_number
        model_url = etree_.SubElement(this_model, dpwsTag('ModelUrl'))
        model_url.text = self.model_url
        presentation_url = etree_.SubElement(this_model, dpwsTag('PresentationUrl'))
        presentation_url.text = self.presentation_url


class DPWSHost:
    __slots__ = ('endpoint_references', 'types')

    def __init__(self, endpoint_references_list, types_list):
        '''
        @param endpoint_references_list: list of WsEndpointReference instances
        @param types_list: a list of etree.QName instances
        '''
        self.endpoint_references = endpoint_references_list
        self.types = types_list

    def as_etree_subnode(self, root_node):
        _ns = Prefixes.partial_map(Prefixes.DPWS, Prefixes.WSA)
        # reverse lookup( key is namespace, value is prefix)
        res = {}
        for key, value in _ns.items():
            res[value] = key
        for key, value in root_node.nsmap.items():
            res[value] = key

        # must explicitely add namespaces of types to Host node, because list of qnames is not handled by lxml
        types_texts = []
        if self.types:
            for qname in self.types:
                prefix = res.get(qname.namespace)
                if not prefix:
                    # create a random prefix
                    prefix = '_dpwsh{}'.format(len(_ns))
                    _ns[prefix] = qname.namespace
                types_texts.append('{}:{}'.format(prefix, qname.localname))

        host_node = etree_.SubElement(root_node, dpwsTag('Host'))  # , nsmap=_ns)
        ep_ref_node = etree_.SubElement(host_node, wsaTag('EndpointReference'))  # , nsmap=_ns)
        for ep_ref in self.endpoint_references:
            ep_ref.as_etree_subnode(ep_ref_node)

        if types_texts:
            types_node = etree_.SubElement(host_node, dpwsTag('Types'),
                                           nsmap=_ns)  # add also namespace prefixes that were locally generated
            types_node.text = ' '.join(types_texts)

    @classmethod
    def from_etree_node(cls, root_node):
        endpoint_references = []
        for tmp in root_node.findall('wsa:EndpointReference', nsmap):
            endpoint_references.append(WsaEndpointReferenceType.from_etree_node(tmp))
        types = _get_text(root_node, 'dpws:Types', nsmap)
        if types:
            types = types.split()
        return cls(endpoint_references, types)

    def __str__(self):
        return 'DPWSHost: endpointReference={}, types="{}"'.format(self.endpoint_references, self.types)


class DPWSHosted:
    __slots__ = ('endpoint_references', 'types', 'service_id', 'soap_client')

    def __init__(self, endpoint_references_list, types_list, service_id):
        self.endpoint_references = endpoint_references_list
        self.types = types_list  # a list of QNames
        self.service_id = service_id
        self.soap_client = None

    def as_etree_subnode(self, root_node):
        hosted_node = etree_.SubElement(root_node, dpwsTag('Hosted'))
        ep_ref_node = etree_.SubElement(hosted_node, wsaTag('EndpointReference'))
        for ep_ref in self.endpoint_references:
            ep_ref.as_etree_subnode(ep_ref_node)
        if self.types:
            types_text = ' '.join([docname_from_qname(t, root_node.nsmap) for t in self.types])
            types_node = etree_.SubElement(hosted_node, dpwsTag('Types'))
            types_node.text = types_text
        service_node = etree_.SubElement(hosted_node, dpwsTag('ServiceId'))
        service_node.text = self.service_id

    @classmethod
    def from_etree_node(cls, root_node):
        endpoint_references = []
        for tmp in root_node.findall('wsa:EndpointReference', nsmap):
            endpoint_references.append(WsaEndpointReferenceType.from_etree_node(tmp))
        types = _get_text(root_node, 'dpws:Types', nsmap)
        if types:
            types = types.split()
        service_id = _get_text(root_node, 'dpws:ServiceId', nsmap)
        return cls(endpoint_references, types, service_id)

    def __str__(self):
        return 'DPWSHosted: endpointReference={}, types="{}" service_id="{}"'.format(self.endpoint_references,
                                                                                     self.types,
                                                                                     self.service_id)


class DPWSRelationShip:
    def __init__(self, root_node=None):
        host_node = root_node.find('dpws:Host', nsmap)
        self.hosted = {}
        self.host = DPWSHost.from_etree_node(host_node)
        for hosted_node in root_node.findall('dpws:Hosted', nsmap):
            hosted = DPWSHosted.from_etree_node(hosted_node)
            self.hosted[hosted.service_id] = hosted


class MetaDataSection:
    def __init__(self, metadata_sections):
        self._metadata_sections = metadata_sections

    def __getattr__(self, attrname):
        try:
            return self._metadata_sections[attrname]
        except KeyError:
            raise AttributeError

    @classmethod
    def from_etree_node(cls, root_node):
        metadata = root_node.find('wsx:Metadata', nsmap)
        metadata_sections = {}
        if metadata is not None:
            for metadata_section_node in metadata.findall('wsx:MetadataSection', nsmap):
                dialect = metadata_section_node.attrib['Dialect']
                if dialect[-1] == '/':
                    dialect = dialect[:-1]
                if dialect == "http://schemas.xmlsoap.org/wsdl":
                    location_node = metadata_section_node.find('wsx:Location', nsmap)
                    metadata_sections['wsdl_location'] = location_node.text
                elif dialect == DIALECT_THIS_MODEL:
                    this_model_node = metadata_section_node.find('dpws:ThisModel', nsmap)
                    metadata_sections['this_model'] = DPWSThisModel.from_etree_node(this_model_node)
                elif dialect == DIALECT_THIS_DEVICE:
                    this_device_node = metadata_section_node.find('dpws:ThisDevice', nsmap)
                    metadata_sections['this_device'] = DPWSThisDevice.from_etree_node(this_device_node)
                elif dialect == DIALECT_RELATIONSHIP:
                    relationship_node = metadata_section_node.find('dpws:Relationship', nsmap)
                    if relationship_node.get('Type') == HOST_TYPE:
                        metadata_sections['relationShip'] = DPWSRelationShip(relationship_node)
        return cls(metadata_sections)


class Soap12EnvelopeBase:
    __slots__ = ('_header_node', '_body_node', '_header_objects', '_body_objects', '_doc_root')

    def __init__(self):
        self._header_node = None
        self._body_node = None
        self._header_objects = []
        self._body_objects = []
        self._doc_root = None

    @property
    def header_node(self):
        return self._header_node

    @property
    def body_node(self):
        return self._body_node

    @staticmethod
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
                msg = "{}\n{}".format(str(err), tmp_str)
                raise ExtendedDocumentInvalid(msg, error_log=err.error_log)


class Soap12Envelope(Soap12EnvelopeBase):
    __slots__ = ('_nsmap', 'address')

    def __init__(self, ns_map):
        super().__init__()
        self._nsmap = ns_map
        self.address = None

    def add_header_object(self, obj):
        assert hasattr(obj, 'as_etree_subnode')
        self._header_objects.append(obj)
        self._doc_root = None

    # def addHeaderString(self, headerString):
    #     element = etree_.fromstring(headerString)
    #     self.add_header_object(GenericNode(element))
    #     self._doc_root = None

    def add_header_element(self, element):
        self.add_header_object(GenericNode(element))
        self._doc_root = None

    def add_body_object(self, obj):
        assert hasattr(obj, 'as_etree_subnode')
        self._body_objects.append(obj)
        self._doc_root = None

    def add_body_string(self, body_string):
        element = etree_.fromstring(body_string)
        self.add_body_object(GenericNode(element))
        self._doc_root = None

    def add_body_element(self, element):
        self.add_body_object(GenericNode(element))
        self._doc_root = None

    def set_address(self, ws_address):
        self.address = ws_address

    def build_doc(self):
        if self._doc_root is not None:
            return self._doc_root

        root = etree_.Element(s12Tag('Envelope'), nsmap=self._nsmap)

        header = etree_.SubElement(root, s12Tag('Header'))
        if self.address:
            self.address.as_etree_subnode(header)
        for header_object in self._header_objects:
            header_object.as_etree_subnode(header)
        body = etree_.SubElement(root, s12Tag('Body'))
        for body_object in self._body_objects:
            body_object.as_etree_subnode(body)
        self._header_node = header
        self._body_node = body
        self._doc_root = root
        return root

    def as_xml(self, pretty=False, request_manipulator=None):
        tmp = BytesIO()
        root = self.build_doc()
        doc = etree_.ElementTree(element=root)
        if hasattr(request_manipulator, 'manipulate_domtree'):
            _doc = request_manipulator.manipulate_domtree(doc)
            if _doc:
                doc = _doc
        doc.write(tmp, encoding='UTF-8', xml_declaration=True, pretty_print=pretty)
        return tmp.getvalue()

    def validate_body(self, schema):
        root = self.build_doc()
        doc = etree_.ElementTree(element=root)
        if CHECK_NAMESPACES:
            self._find_unused_namespaces(root)
            self._find_undefined_namespaces()
        if schema is None:
            return
        body_node = doc.find('s12:Body', nsmap)
        if body_node is not None:
            try:
                payload_node = body_node[0]
            except IndexError:  # empty body
                return
            self._assert_valid_exception_wrapper(schema, payload_node)

    def _find_unused_namespaces(self, root):
        xml_doc = self.as_xml()
        unused = []
        used = []
        for prefix, namespace in root.nsmap.items():
            _pref = prefix + ':'
            if _pref.encode() not in xml_doc:
                unused.append((prefix, namespace))
            else:
                used.append(prefix)
        if unused:
            print(root.nsmap, used, xml_doc[:500])  # do not need to see the wohle message
            raise RuntimeError('unused namespaces:{}, used={}'.format(unused, used))

    def _find_undefined_namespaces(self):
        xml_doc = self.as_xml()
        if b':ns0' in xml_doc:
            raise RuntimeError('undefined namespaces:{}'.format(xml_doc))


class ReceivedSoap12Envelope(Soap12EnvelopeBase):
    __slots__ = ('msg_node', 'rawdata', 'address')

    def __init__(self, doc=None, rawdata=None):
        super().__init__()
        self._doc_root = doc
        self.rawdata = rawdata
        self._header_node = None
        self._body_node = None
        self.address = None
        if doc is not None:
            self._header_node = doc.find('s12:Header', nsmap)
            self._body_node = doc.find('s12:Body', nsmap)
            self.address = WsAddress.from_etree_node(self.header_node)
            try:
                self.msg_node = self.body_node[0]
            except IndexError:  # body has no content, this can happen
                self.msg_node = None

    def as_xml(self, pretty=False):
        tmp = BytesIO()
        doc = etree_.ElementTree(element=self._doc_root)
        doc.write(tmp, encoding='UTF-8', xml_declaration=True, pretty_print=pretty)
        return tmp.getvalue()

    def validate_body(self, schema):
        if schema is None:
            return
        self._assert_valid_exception_wrapper(schema, self.msg_node)

    @classmethod
    def from_xml_string(cls, xml_string, schema=None, **kwargs):
        parser = etree_.ETCompatXMLParser(resolve_entities=False)

        try:
            doc = etree_.fromstring(xml_string, parser=parser, **kwargs)
        except Exception as ex:
            print('load error "{}" in "{}"'.format(ex, xml_string))
            raise
        if schema is not None:
            msg_node = doc.find('s12:Body', nsmap)[0]
            schema.assertValid(msg_node)
        return cls(doc=doc, rawdata=xml_string)


class DPWSEnvelope(ReceivedSoap12Envelope):
    __slots__ = ('address', 'this_model', 'this_device', 'hosted', 'host', 'metadata')

    def __init__(self, doc, rawdata):
        super().__init__(doc, rawdata)
        self.address = None
        self.this_model = None
        self.this_device = None
        self.hosted = {}
        self.host = None
        self.metadata = None

        if doc is not None:
            self.address = WsAddress.from_etree_node(self.header_node)
            self.metadata = MetaDataSection(self.body_node)
            metadata = self.body_node.find('wsx:Metadata', nsmap)
            if metadata is not None:
                for metadata_section_node in metadata.findall('wsx:MetadataSection', nsmap):
                    if metadata_section_node.attrib['Dialect'] == DIALECT_THIS_MODEL:
                        this_model_node = metadata_section_node.find('dpws:ThisModel', nsmap)
                        self.this_model = DPWSThisModel.from_etree_node(this_model_node)
                    elif metadata_section_node.attrib['Dialect'] == DIALECT_THIS_DEVICE:
                        this_device_node = metadata_section_node.find('dpws:ThisDevice', nsmap)
                        self.this_device = DPWSThisDevice.from_etree_node(this_device_node)
                    elif metadata_section_node.attrib['Dialect'] == DIALECT_RELATIONSHIP:
                        relationship = metadata_section_node.find('dpws:Relationship', nsmap)
                        if relationship.get('Type') == HOST_TYPE:
                            host_node = relationship.find('dpws:Host', nsmap)
                            self.host = DPWSHost.from_etree_node(host_node)
                            for hosted_node in relationship.findall('dpws:Hosted', nsmap):
                                hosted = DPWSHosted.from_etree_node(hosted_node)
                                self.hosted[hosted.service_id] = hosted


class _SoapFaultBase(Soap12Envelope):
    '''
    created xml:
        <S:Body>
            <S:Fault>
                <S:Code>
                    <S:Value>[code]</S:Value>
                    <S:Subcode>
                        <S:Value>[subcode]</S:Value>
                    </S:Subcode>
                </S:Code>
                <S:Reason>
                    <S:Text xml:lang="en">[reason]</S:Text>
                </S:Reason>
                <S:Detail>
                    [detail]
                </S:Detail>
            </S:Fault>
        </S:Body>

    '''

    def __init__(self, requestEnvelope, fault_action, code, reason, subCode, details):
        super().__init__(Prefixes.partial_map(Prefixes.S12, Prefixes.WSA, Prefixes.WSE))
        reply_address = requestEnvelope.address.mk_reply_address(fault_action)
        self.add_header_object(reply_address)
        fault_node = etree_.Element(s12Tag('Fault'))
        code_node = etree_.SubElement(fault_node, s12Tag('Code'))
        value_node = etree_.SubElement(code_node, s12Tag('Value'))
        value_node.text = 's12:{}'.format(code)
        if subCode is not None:
            subcode_node = etree_.SubElement(code_node, s12Tag('Subcode'))
            sub_value_node = etree_.SubElement(subcode_node, s12Tag('Value'))
            sub_value_node.text = docname_from_qname(subCode, nsmap)
        reason_node = etree_.SubElement(fault_node, s12Tag('Reason'))
        reason_text_node = etree_.SubElement(reason_node, s12Tag('Text'))
        reason_text_node.set(xmlTag('lang'), 'en-US')
        reason_text_node.text = reason
        if details is not None:
            detail_node = etree_.SubElement(fault_node, s12Tag('Detail'))
            detail_node.set(xmlTag('lang'), 'en-US')
            if isinstance(details, str):
                det_data_node = etree_.SubElement(detail_node, 'data')
                det_data_node.text = details
            else:
                detail_node.append(details)
        self.add_body_element(fault_node)


class SoapFault(_SoapFaultBase):
    SOAP_FAULT_ACTION = '{}/soap/fault'.format(Prefixes.WSA.namespace)

    def __init__(self, requestEnvelope, code, reason, subCode=None, details=None):
        super().__init__(requestEnvelope, self.SOAP_FAULT_ACTION, code, reason, subCode, details)


class AdressingFault(_SoapFaultBase):
    ADDRESSING_FAULT_ACTION = '{}/fault'.format(Prefixes.WSA.namespace)

    def __init__(self, requestEnvelope, code, reason, subCode=None, details=None):
        super().__init__(requestEnvelope, self.ADDRESSING_FAULT_ACTION, code, reason, subCode, details)


class ReceivedSoapFault(ReceivedSoap12Envelope):
    def __init__(self, doc=None, rawdata=None):
        super().__init__(doc, rawdata)
        self.code = ', '.join(self._body_node.xpath('s12:Fault/s12:Code/s12:Value/text()', namespaces=nsmap))
        self.subcode = ', '.join(
            self._body_node.xpath('s12:Fault/s12:Code/s12:Subcode/s12:Value/text()', namespaces=nsmap))
        self.reason = ', '.join(self._body_node.xpath('s12:Fault/s12:Reason/s12:Text/text()', namespaces=nsmap))
        self.detail = ', '.join(self._body_node.xpath('s12:Fault/s12:Detail/text()', namespaces=nsmap))

    def __repr__(self):
        return ('ReceivedSoapFault(code="{}", subcode="{}", reason="{}", detail="{}")'.format(self.code, self.subcode,
                                                                                              self.reason, self.detail))


class SoapFaultCode:
    '''
        Soap Fault codes, see https://www.w3.org/TR/soap12-part1/#faultcodes
    '''
    VERSION_MM = 'VersionMismatch'
    MUSTUNSERSTAND = 'MustUnderstand'
    DATAENC = 'DataEncodingUnknown'
    SENDER = 'Sender'
    RECEIVER = 'Receiver'
