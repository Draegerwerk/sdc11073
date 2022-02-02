import uuid
import copy
from io import BytesIO
from lxml import etree as etree_

from sdc11073.namespaces import wsaTag, wseTag, dpwsTag, s12Tag, xmlTag, nsmap, WSA_ANONYMOUS, docNameFromQName
from sdc11073.namespaces import Prefix_Namespace as Prefix
from .. import isoduration

CHECK_NAMESPACES = False   # can be used to enable additional checks for too many namespaces or undefined namespaces


DIALECT_ACTION = '{}/Action'.format(Prefix.DPWS.namespace)
DIALECT_THIS_MODEL = '{}/ThisModel'.format(Prefix.DPWS.namespace)
DIALECT_THIS_DEVICE = '{}/ThisDevice'.format(Prefix.DPWS.namespace)
DIALECT_RELATIONSHIP = '{}/Relationship'.format(Prefix.DPWS.namespace)
HOST_TYPE = '{}/host'.format(Prefix.DPWS.namespace)


class SoapResponseException(Exception):
    
    def __init__(self, soapResponseEnvelope):
        super(SoapResponseException, self).__init__()
        self.soapResponseEnvelope = soapResponseEnvelope


class ExtendedDocumentInvalid(etree_.DocumentInvalid):

    pass


def mergeDicts(*args):
    result = {}
    for d in args:
        for k, v in d.items():
            if not k in result:
                result[k] = v 
            else:
                if result[k] != v:
                    raise RuntimeError('Merge Conflict key={}, value1={}, value2={}'.format(k, result[k], v))
    return result

    
def getText(node, idstring, ns):
    if node is None:
        return
    tmp = node.find(idstring, ns)
    if tmp is not None:
        return tmp.text


class GenericNode(object):
    def __init__(self, node):
        self._node = node

        
    def asEtreeSubNode(self, rootNode):
        rootNode.append(self._node)


    
class WsaEndpointReferenceType(object):
    ''' Acc. to "http://www.w3.org/2005/08/addressing"

    '''
    __slots__ = ('address', 'referenceParametersNode', 'metaDataNode')
    def __init__(self, address, referenceParametersNode=None, metaDataNode=None):
        self.address = address # type="wsa:AttributedURI", which is an xs:anyURI element
        self.referenceParametersNode = None
        self.metaDataNode = None
        if referenceParametersNode is not None:
            if hasattr(referenceParametersNode, 'tag') and referenceParametersNode.tag == wsaTag('ReferenceParameters'):
                self.referenceParametersNode = referenceParametersNode # any content allowed. optional
            else:
                self.referenceParametersNode = etree_.Element(wsaTag('ReferenceParameters'))
                self.referenceParametersNode.extend(referenceParametersNode)
        if metaDataNode is not None:
            if hasattr(metaDataNode, 'tag') and metaDataNode.tag == wsaTag('MetaData'):
                self.metaDataNode = metaDataNode # any content allowed. optional
            else:
                self.metaDataNode = etree_.Element(wsaTag('MetaData'))
                self.metaDataNode.extend(metaDataNode)

    def __str__(self):
        return 'WsaEndpointReferenceType: address={}'.format(self.address)
    
    @classmethod
    def fromEtreeNode(cls, rootNode):
        addressNode = rootNode.find('wsa:Address', nsmap)
        address = addressNode.text
        referenceParametersNode = rootNode.find('wsa:ReferenceParameters', nsmap)
        metaDataNode = rootNode.find('wsa:MetaData', nsmap)
        ret = cls(address, referenceParametersNode, metaDataNode)
        return ret
    
    
    def asEtreeSubNode(self, rootNode):
        node  = etree_.SubElement(rootNode, wsaTag('Address'))
        node.text = self.address
        if self.referenceParametersNode is not None:
            rootNode.append(copy.copy(self.referenceParametersNode))
        if self.metaDataNode is not None:
            rootNode.append(self.metaDataNode)



class WsAddress(object):
    __slots__ = ('messageId', 'to', 'from_', 'replyTo', 'faultTo', 'action',
                 'messageId', 'relatesTo', 'referenceParametersNode', 'relationshipType')
    def __init__(self, action, messageId=None, to=None, relatesTo=None, from_=None, replyTo=None,
                 faultTo=None, referenceParametersNode=None, relationshipType=None): #pylint: disable=too-many-arguments
        '''

        :param action: xs:anyURI string, required
        :param messageId: xs:anyURI string or None or False; default is None
                          if None, a messageId is generated automatically
                          if False, no message ID is generated ( makes only sense for testing )
        :param to: xs:anyURI string, optional
        :param relatesTo: xs:anyURI string, 0...n
        :param from_: WsaEndpointReferenceType instance, optional
        :param replyTo: WsaEndpointReferenceType instance, optional
        :param faultTo: WsaEndpointReferenceType instance, optional
        :param referenceParametersNode: any node, optional
        :param relationshipType: a QName, optional
        '''
        self.action = action
        if messageId == False:
            self.messageId = None
        else:
            self.messageId = messageId or uuid.uuid4().urn
        self.to = to
        self.relatesTo = relatesTo
        self.from_ = from_
        self.replyTo = replyTo
        self.faultTo = faultTo
        self.referenceParametersNode = referenceParametersNode
        self.relationshipType = relationshipType

    def mkReplyAddress(self, action):
        return WsAddress(action=action, relatesTo=self.messageId)


    def asEtreeSubNode(self, rootNode):
        # To (OPTIONAL), defaults to anonymous
        node = etree_.SubElement(rootNode, wsaTag('To'), attrib={s12Tag('mustUnderstand'): 'true'})
        node.text = self.to or WSA_ANONYMOUS
        #From
        if self.from_:
            self.from_.asEtreeSubNode(rootNode)
        # ReplyTo (OPTIONAL), defaults to anonymous
        if self.replyTo:
            self.replyTo.asEtreeSubNode(rootNode)
        # FaultTo (OPTIONAL)
        if self.faultTo:
            self.faultTo.asEtreeSubNode(rootNode)
        # Action (REQUIRED)
        node  = etree_.SubElement(rootNode, wsaTag('Action'), attrib={s12Tag('mustUnderstand'): 'true'})
        node.text = self.action
        # MessageID (OPTIONAL)
        if self.messageId:
            node  = etree_.SubElement(rootNode, wsaTag('MessageID'))
            node.text = self.messageId
        # RelatesTo (OPTIONAL)
        if self.relatesTo:
            node  = etree_.SubElement(rootNode, wsaTag('RelatesTo'))
            node.text = self.relatesTo
            if self.relationshipType is not None:
                node.set('RelationshipType', self.relationshipType)

        if self.referenceParametersNode:
            rootNode.append(copy.copy(self.referenceParametersNode))

    
    @classmethod
    def fromEtreeNode(cls, rootNode):
        messageId = getText(rootNode, 'wsa:MessageID', nsmap)
        to = getText(rootNode, 'wsa:To', nsmap)
        action = getText(rootNode, 'wsa:Action', nsmap)
        relatesTo = getText(rootNode, 'wsa:RelatesTo', nsmap)
        relationshipType = None
        relatesToNode = rootNode.find('wsa:RelatesTo', nsmap)
        if relatesToNode is not None:
            relatesTo = relatesToNode.text
            relationshipTypeText = relatesToNode.attrib.get('RelationshipType')
            if relationshipTypeText:
                # split into namespace, localname
                ns, loc = relationshipTypeText.rsplit('/', 1)
                relationshipType= etree_.QName(ns, loc)

        def mkEndpointReference(idstring):
            tmp = rootNode.find(idstring, nsmap)
            if tmp is not None:
                return WsaEndpointReferenceType.fromEtreeNode(tmp)
            
        from_ = mkEndpointReference('wsa:From')
        replyTo = mkEndpointReference('wsa:ReplyTo')
        faultTo = mkEndpointReference('wsa:FaultTo')
        referenceParametersNode = rootNode.find('wsa:ReferenceParameters', nsmap)
        
        return cls(messageId=messageId, 
                   to=to, 
                   action=action, 
                   relatesTo=relatesTo, 
                   from_=from_,
                   replyTo=replyTo,
                   faultTo=faultTo,
                   referenceParametersNode=referenceParametersNode,
                   relationshipType=relationshipType)


_LANGUAGE_ATTR = '{http://www.w3.org/XML/1998/namespace}lang'


class WsSubscribe(object):
    MODE_PUSH = '{}/DeliveryModes/Push'.format(Prefix.WSE.namespace)
    __slots__ = ('delivery_mode', 'notifyTo',  'endTo', 'expires', 'filter')
    def __init__(self, notifyTo,
                       expires,
                       endTo=None,
                       filter_=None,
                       delivery_mode=None):
        '''
        @param notifyTo: a WsaEndpointReferenceType
        @param expires: duration in seconds ( absolute date not supported)
        @param endTo: a WsaEndpointReferenceType or None
        @param delivery_mode: defaults to self.MODE_PUSH
        '''
        self.delivery_mode = delivery_mode or self.MODE_PUSH
        self.notifyTo = notifyTo
        self.endTo = endTo
        self.expires = expires
        self.filter = filter_


    def asEtreeSubNode(self, rootNode):
        # To (OPTIONAL), defaults to anonymous
        subscribe = etree_.SubElement(rootNode, wseTag('Subscribe'), nsmap=Prefix.partialMap(Prefix.WSE, Prefix.WSA))
        if self.endTo is not None:
            endToNode = etree_.SubElement(subscribe, wseTag('EndTo'))
            self.endTo.asEtreeSubNode(endToNode)
        delivery = etree_.SubElement(subscribe, wseTag('Delivery'))
        delivery.set('Mode', self.delivery_mode)

        notifyToNode  = etree_.SubElement(delivery, wseTag('NotifyTo'))
        self.notifyTo.asEtreeSubNode(notifyToNode)

        exp = etree_.SubElement(subscribe, wseTag('Expires'))
        exp.text = isoduration.durationString(self.expires)
        fil = etree_.SubElement(subscribe, wseTag('Filter'))
        fil.set('Dialect', DIALECT_ACTION) # Is this always this string?
        fil.text= self.filter


    @classmethod
    def fromEtreeNode(cls, rootNode):
        raise NotImplementedError #pylint: disable=unused-argument



class DPWSThisDevice(object):
    __slots__ = ('friendlyName', 'firmwareVersion', 'serialNumber')

    def __init__(self, friendlyName, firmwareVersion, serialNumber):
        if isinstance(friendlyName, dict):
            self.friendlyName = friendlyName
        else:   
            self.friendlyName = {None: friendlyName} # localized texts
        self.firmwareVersion = firmwareVersion
        self.serialNumber = serialNumber


    @classmethod
    def fromEtreeNode(cls, rootNode):
        friendlyName = {} # localized texts
        for m in rootNode.findall('dpws:FriendlyName', nsmap):
            friendlyName[m.get(_LANGUAGE_ATTR)] = m.text 
        firmwareVersion = getText(rootNode, 'dpws:FirmwareVersion', nsmap)
        serialNumber = getText(rootNode, 'dpws:SerialNumber', nsmap)
        return cls(friendlyName, firmwareVersion, serialNumber)


    def asEtreeSubNode(self, rootNode):
        thisDevice = etree_.SubElement(rootNode, dpwsTag('ThisDevice'), nsmap=Prefix.partialMap(Prefix.DPWS))
        for lang, name in self.friendlyName.items():
            friendlyName = etree_.SubElement(thisDevice, dpwsTag('FriendlyName'))
            friendlyName.text = name
            if lang is not None and len(lang) > 0:
                friendlyName.set(_LANGUAGE_ATTR, lang)
        firmwareVersion = etree_.SubElement(thisDevice, dpwsTag('FirmwareVersion'))
        firmwareVersion.text = self.firmwareVersion
        serialNumber = etree_.SubElement(thisDevice, dpwsTag('SerialNumber'))
        serialNumber.text = self.serialNumber


    def __str__(self):
        return 'DPWSThisDevice: friendlyName={}, firmwareVersion="{}", serialNumber="{}"'.format(self.friendlyName, self.firmwareVersion, self.serialNumber)



class DPWSThisModel(object):
    __slots__ = ('manufacturer', 'manufacturerUrl', 'modelName', 'modelNumber', 'modelUrl', 'presentationUrl')
    def __init__(self, manufacturer, manufacturerUrl, modelName, modelNumber, modelUrl, presentationUrl):
        if isinstance(manufacturer, dict):
            self.manufacturer = manufacturer
        else:   
            self.manufacturer = {None: manufacturer} # localized texts
        self.manufacturerUrl = manufacturerUrl
        if isinstance(modelName, dict):
            self.modelName = modelName
        else:   
            self.modelName = {None: modelName} # localized texts
        self.modelNumber = modelNumber
        self.modelUrl = modelUrl
        self.presentationUrl = presentationUrl


    def __str__(self):
        return 'DPWSThisModel: manufacturer={}, modelName="{}", modelNumber="{}"'.format(self.manufacturer, self.modelName, self.modelNumber)


    @classmethod
    def fromEtreeNode(cls, rootNode):
        manufacturer = {} # localized texts
        for m in rootNode.findall('dpws:Manufacturer', nsmap):
            manufacturer[m.get(_LANGUAGE_ATTR)] = m.text 
        manufacturerUrl = getText(rootNode, 'dpws:ManufacturerUrl', nsmap)
        modelName = {} # localized texts
        for m in rootNode.findall('dpws:ModelName', nsmap):
            modelName[m.get(_LANGUAGE_ATTR)] = m.text 
        modelNumber = getText(rootNode, 'dpws:ModelNumber', nsmap)
        modelUrl = getText(rootNode, 'dpws:ModelUrl',  nsmap)
        presentationUrl = getText(rootNode, 'dpws:PresentationUrl', nsmap)
        return cls(manufacturer, manufacturerUrl, modelName, modelNumber, modelUrl, presentationUrl)


    def asEtreeSubNode(self, rootNode):
        thisModel = etree_.SubElement(rootNode, dpwsTag('ThisModel'), nsmap=Prefix.partialMap(Prefix.DPWS))
        for lang, name in self.manufacturer.items():
            manufacturer = etree_.SubElement(thisModel, dpwsTag('Manufacturer'))
            manufacturer.text = name
            if lang is not None and len(lang) > 0:
                manufacturer.set(_LANGUAGE_ATTR, lang)

        manufacturerUrl = etree_.SubElement(thisModel, dpwsTag('ManufacturerUrl'))
        manufacturerUrl.text = self.manufacturerUrl

        for lang, name in self.modelName.items():
            manufacturer = etree_.SubElement(thisModel, dpwsTag('ModelName'))
            manufacturer.text = name
            if lang is not None:
                manufacturer.set(_LANGUAGE_ATTR, lang)

        modelNumber = etree_.SubElement(thisModel, dpwsTag('ModelNumber'))
        modelNumber.text = self.modelNumber
        modelUrl = etree_.SubElement(thisModel, dpwsTag('ModelUrl'))
        modelUrl.text = self.modelUrl
        presentationUrl = etree_.SubElement(thisModel, dpwsTag('PresentationUrl'))
        presentationUrl.text = self.presentationUrl



class DPWSHost(object):
    __slots__ = ('endpointReferences', 'types')
    def __init__(self, endpointReferencesList, typesList):
        '''
        @param endpointReferencesList: list of WsEndpointReference instances
        @param typesList: a list of etree.QName instances
        '''
        self.endpointReferences = endpointReferencesList
        self.types = typesList


    def asEtreeSubNode(self, rootNode):
        _ns = Prefix.partialMap(Prefix.DPWS, Prefix.WSA)
        # reverse lookup( key is namespace, value is prefix)
        res = {}
        for k,v in _ns.items():
            res[v] = k
        for k,v in rootNode.nsmap.items():
            res[v] = k
        
        # must explicitely add namespaces of types to Host node, because list of qnames is not handled by lxml
        typesTexts = []
        if self.types:
            for qname in self.types:
                prefix = res.get(qname.namespace)
                if not prefix:
                    # create a random prefix
                    prefix='_dpwsh{}'.format(len(_ns))
                    _ns[prefix] = qname.namespace
                typesTexts.append('{}:{}'.format(prefix, qname.localname))
                
        hostNode = etree_.SubElement(rootNode, dpwsTag('Host'))#, nsmap=_ns)
        epRefNode = etree_.SubElement(hostNode, wsaTag('EndpointReference'))#, nsmap=_ns) 
        for epRef in self.endpointReferences:
            epRef.asEtreeSubNode(epRefNode)
            
        if typesTexts:
            typesNode = etree_.SubElement(hostNode, dpwsTag('Types'), nsmap=_ns)# add also namespace prefixes that were locally generated
            typesText = ' '.join(typesTexts)
            typesNode.text = typesText


    @classmethod
    def fromEtreeNode(cls, rootNode):
        endpointReferences = []
        for tmp in rootNode.findall('wsa:EndpointReference', nsmap):
            endpointReferences.append(WsaEndpointReferenceType.fromEtreeNode(tmp))
        types = getText(rootNode, 'dpws:Types', nsmap)
        if types:
            types = types.split()
        return cls(endpointReferences, types)


    def __str__(self):
        return 'DPWSHost: endpointReference={}, types="{}"'.format(self.endpointReferences, self.types)



class DPWSHosted(object):
    __slots__ = ('endpointReferences', 'types', 'serviceId', 'soapClient')
    def __init__(self, endpointReferencesList, typesList, serviceId):
        self.endpointReferences = endpointReferencesList
        self.types = typesList  # a list of QNames
        self.serviceId = serviceId
        self.soapClient = None


    def asEtreeSubNode(self, rootNode):
        hostedNode = etree_.SubElement(rootNode, dpwsTag('Hosted'))
        epRefNode = etree_.SubElement(hostedNode, wsaTag('EndpointReference'))
        for epRef in self.endpointReferences:
            epRef.asEtreeSubNode(epRefNode)
        if self.types:
            typesText = ' '.join([docNameFromQName(t, rootNode.nsmap) for t in self.types])
            typesNode = etree_.SubElement(hostedNode, dpwsTag('Types'))#, nsmap=ns)
            typesNode.text = typesText
        serviceNode = etree_.SubElement(hostedNode, dpwsTag('ServiceId'))#, nsmap=ns)
        serviceNode.text = self.serviceId


    @classmethod
    def fromEtreeNode(cls, rootNode):
        endpointReferences = []
        for tmp in rootNode.findall('wsa:EndpointReference', nsmap):
            endpointReferences.append(WsaEndpointReferenceType.fromEtreeNode(tmp))
        types = getText(rootNode, 'dpws:Types', nsmap)
        if types:
            types = types.split()
        serviceId = getText(rootNode, 'dpws:ServiceId', nsmap)
        return cls(endpointReferences, types, serviceId)

    def __str__(self):
        return 'DPWSHosted: endpointReference={}, types="{}" serviceId="{}"'.format(self.endpointReferences, self.types, self.serviceId)



class DPWSRelationShip(object):
    def __init__(self, rootNode=None):
        hostNode = rootNode.find('dpws:Host', nsmap)
        self.hosted = {}
        self.host = DPWSHost.fromEtreeNode(hostNode)
        for hostedNode in rootNode.findall('dpws:Hosted', nsmap):
            hosted = DPWSHosted.fromEtreeNode(hostedNode)
            self.hosted[hosted.serviceId] = hosted



class MetaDataSection(object):
    def __init__(self, metadataSections):
        self._metadataSections = metadataSections


    def __getattr__(self, attrname):
        try:
            return self._metadataSections[attrname]
        except KeyError:
            raise AttributeError


    @classmethod
    def fromEtreeNode(cls, rootNode):
        metadata = rootNode.find('wsx:Metadata', nsmap)
        metadataSections = {}
        if metadata is not None:
            for metadataSection in metadata.findall('wsx:MetadataSection', nsmap):
                dialect = metadataSection.attrib['Dialect']
                if dialect[-1] == '/': 
                    dialect = dialect[:-1]
                if dialect == "http://schemas.xmlsoap.org/wsdl":
                    locationNode = metadataSection.find('wsx:Location', nsmap)
                    metadataSections['wsdl_location'] = locationNode.text
                elif dialect == DIALECT_THIS_MODEL:
                    thisModelNode = metadataSection.find('dpws:ThisModel', nsmap)
                    metadataSections['thisModel'] = DPWSThisModel.fromEtreeNode(thisModelNode)
                elif dialect == DIALECT_THIS_DEVICE:
                    thisDeviceNode = metadataSection.find('dpws:ThisDevice', nsmap)
                    metadataSections['thisDevice'] = DPWSThisDevice.fromEtreeNode(thisDeviceNode)
                elif dialect == DIALECT_RELATIONSHIP:
                    relationshipNode = metadataSection.find('dpws:Relationship', nsmap)
                    if relationshipNode.get('Type') == HOST_TYPE:
                        metadataSections['relationShip'] = DPWSRelationShip(relationshipNode)
        return cls(metadataSections)


class Soap12EnvelopeBase(object):
    __slots__ = ('_headerNode', '_bodyNode', '_headerObjects', '_bodyObjects', '_docRoot')
    def __init__(self):
        self._headerNode = None
        self._bodyNode = None
        self._headerObjects = []
        self._bodyObjects = []
        self._docRoot = None

    @property
    def headerNode(self):
        return self._headerNode

    @property
    def bodyNode(self):
        return self._bodyNode

    def _assert_valid_exception_wrapper(self, schema, content):
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
    def __init__(self, nsmap):
        super(Soap12Envelope, self).__init__()
        self._nsmap = nsmap
        self.address = None

    def addHeaderObject(self, obj):
        assert hasattr(obj, 'asEtreeSubNode')
        self._headerObjects.append(obj)
        self._docRoot = None

    def addHeaderString(self, headerString):
        element = etree_.fromstring(headerString)
        self.addHeaderObject(GenericNode(element))
        self._docRoot = None
        
    def addHeaderElement(self, element):
        self.addHeaderObject(GenericNode(element))
        self._docRoot = None
    
    def addBodyObject(self, obj):
        assert hasattr(obj, 'asEtreeSubNode')
        self._bodyObjects.append(obj)
        self._docRoot = None

    def addBodyString(self, bodyString):
        element = etree_.fromstring(bodyString)
        self.addBodyObject(GenericNode(element))
        self._docRoot = None

    def addBodyElement(self, element):
        self.addBodyObject(GenericNode(element))
        self._docRoot = None

    def setAddress(self, wsAddress):
        self.address = wsAddress

    def buildDoc(self):
        if self._docRoot is not None:
            return self._docRoot
        
        root = etree_.Element(s12Tag('Envelope'), nsmap=self._nsmap)

        header = etree_.SubElement(root, s12Tag('Header'))
        if self.address:
            self.address.asEtreeSubNode(header)
        for h in self._headerObjects:
            h.asEtreeSubNode(header)
        body = etree_.SubElement(root, s12Tag('Body'))
        for b in self._bodyObjects:
            b.asEtreeSubNode(body)
        self._headerNode = header
        self._bodyNode = body
        self._docRoot = root
        return root

    def as_xml(self, pretty=False, request_manipulator=None):
        tmp = BytesIO()
        root = self.buildDoc()
        doc = etree_.ElementTree(element=root)
        if hasattr(request_manipulator, 'manipulate_domtree'):
            _doc = request_manipulator.manipulate_domtree(doc)
            if _doc:
                doc = _doc
        doc.write(tmp, encoding='UTF-8', xml_declaration=True, pretty_print=pretty)
        return tmp.getvalue()

    def validate_envelope(self, xml_validator):
        if xml_validator is None:
            return
        root = self.buildDoc()
        doc = etree_.ElementTree(element=root)
        xml_validator.assertValid(doc)
        if CHECK_NAMESPACES:
            self._find_unused_namespaces(root)
            self._find_undefined_namespaces()
        self._assert_valid_exception_wrapper(xml_validator, doc)

    def _find_unused_namespaces(self, root):
        xml_doc = self.as_xml()
        unused = []
        used = []
        for prefix, ns in root.nsmap.items():
            _pr = prefix+':'
            if _pr.encode() not in xml_doc:
                unused.append((prefix, ns))
            else:
                used.append(prefix)
        if unused:
            print (root.nsmap, used, xml_doc[:500]) # do not need to see the wohle message
            raise RuntimeError('unused namespaces:{}, used={}'.format(unused, used))

    def _find_undefined_namespaces(self):
        xml_doc = self.as_xml()
        if b':ns0' in xml_doc:
            raise RuntimeError('undefined namespaces:{}'.format(xml_doc))


class ReceivedSoap12Envelope(Soap12EnvelopeBase):
    __slots__ = ('msgNode', 'rawdata', 'address')
    def __init__(self, doc=None, rawdata=None):
        super(ReceivedSoap12Envelope, self).__init__()
        self._docRoot = doc
        self.rawdata = rawdata
        self._headerNode = None
        self._bodyNode = None
        self.address = None
        if doc is not None:
            self._headerNode = doc.find('s12:Header', nsmap)
            self._bodyNode = doc.find('s12:Body', nsmap)
            self.address = WsAddress.fromEtreeNode(self.headerNode)
            try:
                self.msgNode = self.bodyNode[0]
            except IndexError: # body has no content, this can happen
                self.msgNode = None
        

    def as_xml(self, pretty=False):
        tmp = BytesIO()
        doc = etree_.ElementTree(element=self._docRoot)
        doc.write(tmp, encoding='UTF-8', xml_declaration=True, pretty_print=pretty)
        return tmp.getvalue()

    def validate_envelope(self, xml_validator):
        if xml_validator is None:
            return
        self._assert_valid_exception_wrapper(xml_validator, self._docRoot)

    @classmethod
    def fromXMLString(cls, xmlString):
        parser = etree_.ETCompatXMLParser()
        
        try:    
            doc = etree_.fromstring(xmlString, parser=parser)
        except Exception as ex:
            print ('load error "{}" in "{}"'.format(ex, xmlString))
            raise
        return cls(doc=doc, rawdata=xmlString)



class DPWSEnvelope(ReceivedSoap12Envelope):
    __slots__ = ('address', 'thisModel', 'thisDevice', 'hosted', 'host', 'metaData')

    def __init__(self, doc, rawdata):
        super(DPWSEnvelope, self).__init__(doc, rawdata)
        self.address = None
        self.thisModel = None
        self.thisDevice = None
        self.hosted = {}
        self.host = None
        self.metaData = None
        
        if doc is not None:
            self.address = WsAddress.fromEtreeNode(self.headerNode)
            self.metaData = MetaDataSection(self.bodyNode)
            metadata = self.bodyNode.find('wsx:Metadata', nsmap)
            if metadata is not None:
                for metadataSection in metadata.findall('wsx:MetadataSection', nsmap):
                    if metadataSection.attrib['Dialect'] == DIALECT_THIS_MODEL:
                        thisModelNode = metadataSection.find('dpws:ThisModel', nsmap)
                        self.thisModel = DPWSThisModel.fromEtreeNode(thisModelNode)
                    elif metadataSection.attrib['Dialect'] == DIALECT_THIS_DEVICE:
                        thisDeviceNode = metadataSection.find('dpws:ThisDevice', nsmap)
                        self.thisDevice = DPWSThisDevice.fromEtreeNode(thisDeviceNode)
                    elif metadataSection.attrib['Dialect'] == DIALECT_RELATIONSHIP:
                        relationship = metadataSection.find('dpws:Relationship', nsmap)
                        if relationship.get('Type') == HOST_TYPE:
                            hostNode = relationship.find('dpws:Host', nsmap)
                            self.host = DPWSHost.fromEtreeNode(hostNode)
                            for hostedNode in relationship.findall('dpws:Hosted', nsmap):
                                hosted = DPWSHosted.fromEtreeNode(hostedNode)
                                self.hosted[hosted.serviceId] = hosted


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
        super(_SoapFaultBase, self).__init__(Prefix.partialMap(Prefix.S12, Prefix.WSA,Prefix.WSE))
        replyAddress = requestEnvelope.address.mkReplyAddress(fault_action)
        self.addHeaderObject(replyAddress)
        faultNode = etree_.Element(s12Tag('Fault'))
        codeNode = etree_.SubElement(faultNode, s12Tag('Code'))
        valueNode = etree_.SubElement(codeNode, s12Tag('Value'))
        valueNode.text = 's12:{}'.format(code)
        if subCode is not None:
            subcodeNode = etree_.SubElement(codeNode, s12Tag('Subcode'))
            valueNode = etree_.SubElement(subcodeNode, s12Tag('Value'))
            valueNode.text = docNameFromQName(subCode, nsmap)
        reasonNode = etree_.SubElement(faultNode, s12Tag('Reason'))
        reasontextNode = etree_.SubElement(reasonNode, s12Tag('Text'))
        reasontextNode.set(xmlTag('lang'), 'en-US')
        reasontextNode.text = reason
        if details is not None:
            _detailNode = etree_.SubElement(faultNode, s12Tag('Detail'))
            _detailNode.set(xmlTag('lang'), 'en-US')
            if isinstance(details, str):
                detNode = etree_.SubElement(_detailNode, 'data')
                detNode.text = details
            else:
                _detailNode.append(details)
        self.addBodyElement(faultNode)


class SoapFault(_SoapFaultBase):
    SOAP_FAULT_ACTION = '{}/soap/fault'.format(Prefix.WSA.namespace)
    def __init__(self, requestEnvelope, code, reason, subCode=None, details=None):
        super(SoapFault, self).__init__(requestEnvelope, self.SOAP_FAULT_ACTION, code, reason, subCode, details)


class AdressingFault(_SoapFaultBase):
    ADDRESSING_FAULT_ACTION = '{}/fault'.format(Prefix.WSA.namespace)
    def __init__(self, requestEnvelope, code, reason, subCode=None, details=None):
        super(AdressingFault, self).__init__(requestEnvelope, self.ADDRESSING_FAULT_ACTION, code, reason, subCode, details)


class ReceivedSoapFault(ReceivedSoap12Envelope):
    def __init__(self, doc=None, rawdata=None):
        super(ReceivedSoapFault, self).__init__(doc, rawdata)
        self.code = ', '.join(self._bodyNode.xpath('s12:Fault/s12:Code/s12:Value/text()', namespaces=nsmap))
        self.subcode = ', '.join(self._bodyNode.xpath('s12:Fault/s12:Code/s12:Subcode/s12:Value/text()', namespaces=nsmap))
        self.reason = ', '.join(self._bodyNode.xpath('s12:Fault/s12:Reason/s12:Text/text()', namespaces=nsmap))
        self.detail = ', '.join(self._bodyNode.xpath('s12:Fault/s12:Detail/text()', namespaces=nsmap))

    def __repr__(self):
        return ('ReceivedSoapFault(code="{}", subcode="{}", reason="{}", detail="{}")'.format(self.code, self.subcode, self.reason, self.detail))


class SoapFaultCode:
    '''
        Soap Fault codes, see https://www.w3.org/TR/soap12-part1/#faultcodes
    '''
    VERSION_MM = 'VersionMismatch'
    MUSTUNSERSTAND = 'MustUnderstand'
    DATAENC = 'DataEncodingUnknown'
    SENDER = 'Sender'
    RECEIVER = 'Receiver'
