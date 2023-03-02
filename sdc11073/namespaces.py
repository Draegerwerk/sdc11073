""" A helper for xml name space handling"""
from collections import namedtuple
from enum import Enum
from typing import Optional, Type

from lxml import etree as etree_

_PrefixNamespaceTuple = namedtuple('_PrefixNamespaceTuple', 'prefix namespace')


class PrefixNamespace(_PrefixNamespaceTuple):

    def tag(self, localname: str) -> etree_.QName:
        return etree_.QName(self.namespace, localname)

    def doc_name(self, localname: str) -> str:
        if self.prefix:
            return f'{self.prefix}:{localname}'
        return localname


# these are internally used namespaces, they are not all identical the ones that are used in sdc.
# it is in the responsibility of the sdc definitions class(es) to convert between internal and external namespaces.
# Originally this abstraction was needed, because during development of the sdc standard the namespaces changed.
# Although the standard is meanwhile final, this abstraction might again be needed if in the future a new revision
# of the standard appears.
class PrefixesEnum(PrefixNamespace, Enum):
    MSG = PrefixNamespace('msg', 'http://standards.ieee.org/downloads/11073/11073-10207-2017/message')
    PM = PrefixNamespace('dom', 'http://standards.ieee.org/downloads/11073/11073-10207-2017/participant')
    MDPWS = PrefixNamespace('mdpws', 'http://standards.ieee.org/downloads/11073/11073-20702-2016')
    EXT = PrefixNamespace('ext', 'http://standards.ieee.org/downloads/11073/11073-10207-2017/extension')
    SDC = PrefixNamespace('sdc', 'http://standards.ieee.org/downloads/11073/11073-20701-2018')
    WSE = PrefixNamespace('wse', 'http://schemas.xmlsoap.org/ws/2004/08/eventing')
    XSD = PrefixNamespace('xsd', 'http://www.w3.org/2001/XMLSchema')
    XSI = PrefixNamespace('xsi', 'http://www.w3.org/2001/XMLSchema-instance')
    WSA = PrefixNamespace('wsa', 'http://www.w3.org/2005/08/addressing')
    WSX = PrefixNamespace('wsx', 'http://schemas.xmlsoap.org/ws/2004/09/mex')  # Meta Data Exchange
    DPWS = PrefixNamespace('dpws', 'http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01')
    WSD = PrefixNamespace('wsd', 'http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01')
    S12 = PrefixNamespace('s12', 'http://www.w3.org/2003/05/soap-envelope')
    XML = PrefixNamespace('xml', 'http://www.w3.org/XML/1998/namespace')
    WXF = PrefixNamespace('wxf', 'http://schemas.xmlsoap.org/ws/2004/09/transfer')  # ws-transfer
    WSDL = PrefixNamespace('wsdl', 'http://schemas.xmlsoap.org/wsdl/')
    WSDL12 = PrefixNamespace('wsdl12', 'http://schemas.xmlsoap.org/wsdl/soap12/')  # old soap 12 namespace, used in wsdl 1.1. only for wsdl
    WSP = PrefixNamespace('wsp', 'http://www.w3.org/ns/ws-policy')


class NamespaceHelper:
    def __init__(self, prefixes_enum: Type[PrefixesEnum], default_ns:Optional[str]=None):
        self.prefix_enum = prefixes_enum
        self._lookup = {}
        for enum_item in prefixes_enum:
            self._lookup[enum_item.name] = enum_item.value

        self._default_ns = default_ns

        self._prefix_map = dict((x.namespace, x.prefix) for x in self._lookup.values())  # map namespace to prefix
        self.ns_map = dict((x.prefix, x.namespace) for x in self._lookup.values())  # map prefix to namespace

    @property
    def nsmap(self) -> dict:
        return self.ns_map

    @property
    def MSG(self) -> PrefixNamespace:
        return self._lookup['MSG']

    def msgTag(self, tag_name) -> etree_.QName:
        return self._tag(self.MSG, tag_name)

    @property
    def PM(self) -> PrefixNamespace:
        return self._lookup['PM']

    def domTag(self, tag_name) -> etree_.QName:
        return self._tag(self.PM, tag_name)

    @property
    def EXT(self) -> PrefixNamespace:
        return self._lookup['EXT']

    def extTag(self, tag_name) -> etree_.QName:
        return self.EXT.tag(tag_name)

    @property
    def SDC(self) -> PrefixNamespace:
        return self._lookup['SDC']

    @property
    def WSE(self) -> PrefixNamespace:
        return self._lookup['WSE']

    def wseTag(self, tag_name) -> etree_.QName:
        return self.WSE.tag(tag_name)

    @property
    def XSI(self) -> PrefixNamespace:
        return self._lookup['XSI']

    def xsiTag(self, tag_name) -> etree_.QName:
        return self.XSI.tag(tag_name)

    @property
    def WSA(self) -> PrefixNamespace:
        return self._lookup['WSA']

    def wsaTag(self, tag_name) -> etree_.QName:
        return self.WSA.tag(tag_name)

    @property
    def WSX(self) -> PrefixNamespace:
        return self._lookup['WSX']

    def wsxTag(self, tag_name) -> etree_.QName:
        return self.WSX.tag(tag_name)

    @property
    def DPWS(self) -> PrefixNamespace:
        return self._lookup['DPWS']

    def dpwsTag(self, tag_name) -> etree_.QName:
        return self.DPWS.tag(tag_name)

    @property
    def MDPWS(self) -> PrefixNamespace:
        return self._lookup['MDPWS']

    def mdpwsTag(self, tag_name) -> etree_.QName:
        return self.MDPWS.tag(tag_name)

    def siTag(self, tag_name) -> etree_.QName:
        return self.MDPWS.tag(tag_name)  # maps to MDPWS

    @property
    def WSD(self) -> PrefixNamespace:
        return self._lookup['WSD']

    def wsdTag(self, tag_name) -> etree_.QName:
        return self.WSD.tag(tag_name)

    @property
    def S12(self) -> PrefixNamespace:
        return self._lookup['S12']

    def s12Tag(self, tag_name) -> etree_.QName:
        return self.S12.tag(tag_name)

    @property
    def XML(self) -> PrefixNamespace:
        return self._lookup['XML']

    def xmlTag(self, tag_name) -> etree_.QName:
        return self.XML.tag(tag_name)

    @property
    def WSDL(self) -> PrefixNamespace:
        return self._lookup['WSDL']

    def wsdlTag(self, tag_name) -> etree_.QName:
        return self.WSDL.tag(tag_name)

    @property
    def WSDL12(self) -> PrefixNamespace:
        return self._lookup['WSDL12']

    def wsdl12Tag(self, tag_name) -> etree_.QName:
        return self.WSDL12.tag(tag_name)

    @property
    def WSP(self) -> PrefixNamespace:
        return self._lookup['WSP']

    def wspTag(self, tag_name) -> etree_.QName:
        return self.WSP.tag(tag_name)

    @property
    def WXF(self) -> PrefixNamespace:
        return self._lookup['WXF']

    @property
    def XSD(self) -> PrefixNamespace:
        return self._lookup['XSD']

    def partial_map(self, *prefix) -> dict:
        """
        :param prefix: Prefix_Namespace_Tuples
        :param default: if given, the default name space
        :return: a dictionary with prefix as key, namespace as value
        """
        # ret = dict((v.prefix, v.namespace) for v in prefix)
        # if default is not None:
        #     ret[None] = default.namespace
        # return ret
        ret = {}
        for p in prefix:
            if p.namespace == self._default_ns:
                ret[None] = p.namespace
            ret[p.prefix] = p.namespace
        return ret

    def doc_name_from_qname(self, qname: etree_.QName) -> str:
        """ returns the prefix:name string, or only name (if default namespace is used) """
        if qname.namespace is not None and qname.namespace == self._default_ns:
            return qname.localname
        prefix = self._prefix_map[qname.namespace]
        return f'{prefix}:{qname.localname}'

    def text_to_qname(self, text: str, nsmap: dict = None) -> etree_.QName:
        ns_map = nsmap or self.ns_map
        elements = text.split(':')
        prefix = None if len(elements) == 1 else elements[0]
        name = elements[-1]
        try:
            return etree_.QName(ns_map[prefix], name)
        except KeyError as ex:
            raise KeyError(f'Cannot make QName for {text}, prefix is not in nsmap: {ns_map.keys()}') from ex

    def _tag(self, prefix_namespace: PrefixNamespace, localname: str) -> etree_.QName:
        return etree_.QName(prefix_namespace.namespace, localname)


default_ns_helper = NamespaceHelper(PrefixesEnum) #, default_ns=PrefixesEnum.PM.namespace)

nsmap = default_ns_helper.ns_map

# some constants from ws-addressing
WSA_ANONYMOUS = PrefixesEnum.WSA.namespace + '/anonymous'
WSA_NONE = PrefixesEnum.WSA.namespace + '/none'


def docname_from_qname(qname: etree_.QName, ns_map: dict) -> str:
    """ returns prefix:name string, or only name (if default namespace is used) """
    prefixmap = dict((v, k) for k, v in ns_map.items())
    prefix = prefixmap.get(qname.namespace)
    if prefix is None:
        return qname.localname
    return f'{prefix}:{qname.localname}'


def text_to_qname(text: str, doc_nsmap: dict) -> etree_.QName:
    elements = text.split(':')
    prefix = None if len(elements) == 1 else elements[0]
    name = elements[-1]
    try:
        return etree_.QName(doc_nsmap[prefix], name)
    except KeyError as ex:
        raise KeyError(f'Cannot make QName for {text}, prefix is not in nsmap: {doc_nsmap.keys()}') from ex


QN_TYPE = etree_.QName(PrefixesEnum.XSI.namespace, 'type')  # frequently used QName, central definition


class EventingActions:
    Subscribe = PrefixesEnum.WSE.namespace + '/Subscribe'
    SubscribeResponse = PrefixesEnum.WSE.namespace + '/SubscribeResponse'
    SubscriptionEnd = PrefixesEnum.WSE.namespace + '/SubscriptionEnd'
    Unsubscribe = PrefixesEnum.WSE.namespace + '/Unsubscribe'
    UnsubscribeResponse = PrefixesEnum.WSE.namespace + '/UnsubscribeResponse'
    Renew = PrefixesEnum.WSE.namespace + '/Renew'
    RenewResponse = PrefixesEnum.WSE.namespace + '/RenewResponse'
    GetStatus = PrefixesEnum.WSE.namespace + '/GetStatus'
    GetStatusResponse = PrefixesEnum.WSE.namespace + '/GetStatusResponse'
