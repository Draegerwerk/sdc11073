""" A helper for xml name space handling"""
from collections import namedtuple
from enum import Enum
from functools import partial

from lxml import etree as etree_

_PrefixNamespaceTuple = namedtuple('_PrefixNamespaceTuple', 'prefix namespace')


# these are internally used namespaces, they are not all identical the ones that are used in sdc.
# it is in the responsibility of the sdc definitions class(es) to convert between internal and external namespaces.
# Originally this abstraction was needed, because during development of the sdc standard the namespaces changed.
# Although the standard is meanwhile final, this abstraction might again be needed if in the future a new revision
# of the standard appears.
class Prefixes(_PrefixNamespaceTuple, Enum):
    MSG = _PrefixNamespaceTuple('msg', "__BICEPS_MessageModel__")
    PM = _PrefixNamespaceTuple('dom', "__BICEPS_ParticipantModel__")
    MDPWS = _PrefixNamespaceTuple('mdpws', "__MDPWS__")
    EXT = _PrefixNamespaceTuple('ext', "__ExtensionPoint__")
    SDC = _PrefixNamespaceTuple('sdc', 'http://standards.ieee.org/downloads/11073/11073-20701-2018')
    WSE = _PrefixNamespaceTuple('wse', 'http://schemas.xmlsoap.org/ws/2004/08/eventing')
    XSD = _PrefixNamespaceTuple('xsd', 'http://www.w3.org/2001/XMLSchema')
    XSI = _PrefixNamespaceTuple('xsi', 'http://www.w3.org/2001/XMLSchema-instance')
    WSA = _PrefixNamespaceTuple('wsa', 'http://www.w3.org/2005/08/addressing')
    WSX = _PrefixNamespaceTuple('wsx', 'http://schemas.xmlsoap.org/ws/2004/09/mex')  # Meta Data Exchange
    DPWS = _PrefixNamespaceTuple('dpws', 'http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01')
    WSD = _PrefixNamespaceTuple('wsd', 'http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01')
    S12 = _PrefixNamespaceTuple('s12', 'http://www.w3.org/2003/05/soap-envelope')
    XML = _PrefixNamespaceTuple('xml', 'http://www.w3.org/XML/1998/namespace')
    WXF = _PrefixNamespaceTuple('wxf', 'http://schemas.xmlsoap.org/ws/2004/09/transfer')  # ws-transfer
    WSDL = _PrefixNamespaceTuple('wsdl', 'http://schemas.xmlsoap.org/wsdl/')

    @staticmethod
    def partial_map(*prefix):
        """
        :param prefix: Prefix_Namespace_Tuples
        :return: a dictionary with prefix as key, namespace as value
        """
        return dict((v.prefix, v.namespace) for v in prefix)


# Prefix_Namespace = Prefixes
# these are all namespaces used in sdc:
nsmap = dict((item.prefix, item.namespace) for item in Prefixes)


def _tag_name(prefix_namespace_tuple, tagname):
    return etree_.QName(prefix_namespace_tuple.namespace, tagname)


msgTag = partial(_tag_name, Prefixes.MSG)  # a helper to make qualified names
domTag = partial(_tag_name, Prefixes.PM)  # a helper to make qualified names
extTag = partial(_tag_name, Prefixes.EXT)  # a helper to make qualified names
wseTag = partial(_tag_name, Prefixes.WSE)  # a helper to make qualified names
xsiTag = partial(_tag_name, Prefixes.XSI)  # a helper to make qualified names
wsaTag = partial(_tag_name, Prefixes.WSA)  # a helper to make qualified names
wsxTag = partial(_tag_name, Prefixes.WSX)  # a helper to make qualified names
dpwsTag = partial(_tag_name, Prefixes.DPWS)  # a helper to make qualified names
mdpwsTag = partial(_tag_name, Prefixes.MDPWS)  # a helper to make qualified names
siTag = partial(_tag_name, Prefixes.MDPWS)  # a helper to make qualified names
wsdTag = partial(_tag_name, Prefixes.WSD)  # a helper to make qualified names
s12Tag = partial(_tag_name, Prefixes.S12)  # a helper to make qualified names
xmlTag = partial(_tag_name, Prefixes.XML)  # a helper to make qualified names

# some constants from ws-addressing
WSA_ANONYMOUS = Prefixes.WSA.namespace + '/anonymous'
WSA_NONE = Prefixes.WSA.namespace + '/none'


def docname_from_qname(qname, ns_map):
    """ returns the docprefix:name string, or only name (if default namespace is used) """
    prefixmap = dict((v, k) for k, v in ns_map.items())
    prefix = prefixmap[qname.namespace]
    if prefix is None:
        return qname.localname
    return f'{prefix}:{qname.localname}'


class DocNamespaceHelper:
    def __init__(self):
        self._prefixmap = dict((x.namespace, x.prefix) for x in Prefixes)

    def use_doc_prefixes(self, document_nsmap):
        for prefix, _ns in document_nsmap.items():
            self._prefixmap[_ns] = prefix

    def _doc_prefix(self, prefix_namespace_tuple):
        """ returns the document prefix for nsmap prefix"""
        return self._prefixmap[prefix_namespace_tuple.namespace]

    def msg_prefix(self):
        """

        :return: default Prefix of Message Model
        """
        return self._prefixmap[Prefixes.MSG.namespace]

    def dom_prefix(self):
        """

        :return: default Prefix of Participant Model
        """
        return self._prefixmap[Prefixes.PM.namespace]

    def doc_name(self, my_refix_or_namespace, name):
        """ returns the docprefix:name string. """
        prefix = self._doc_prefix(my_refix_or_namespace)
        if prefix is None:
            return name
        return f'{prefix}:{name}'

    def docname_from_qname(self, qname):
        """ returns the docprefix:name string, or only name (if default namespace is used) """
        prefix = self._prefixmap[qname.namespace]
        if prefix is None:
            return qname.localname
        return f'{prefix}:{qname.localname}'

    @property
    def doc_ns_map(self):
        return dict((v, k) for k, v in self._prefixmap.items())

    def partial_map(self, *prefixes):
        """
        :param prefix: Prefix class members
        :return: a dictionary with prefix as key, namespace as value
        """
        my_namespaces = []
        for prefix in prefixes:
            if isinstance(prefix, _PrefixNamespaceTuple):
                my_namespaces.append(prefix.namespace)
            else:
                my_namespaces.append(nsmap[prefix])
        return dict((v, k) for k, v in self._prefixmap.items() if k in my_namespaces)


QN_TYPE = xsiTag('type')  # frequently used QName, central definition


def text_to_qname(text, doc_nsmap):
    elements = text.split(':')
    prefix = None if len(elements) == 1 else elements[0]
    name = elements[-1]
    try:
        return etree_.QName(doc_nsmap[prefix], name)
    except KeyError as ex:
        raise KeyError(f'Cannot make QName for {text}, prefix is not in nsmap: {doc_nsmap.keys()}') from ex


class EventingActions:
    Subscribe = Prefixes.WSE.namespace + '/Subscribe'
    SubscribeResponse = Prefixes.WSE.namespace + '/SubscribeResponse'
    SubscriptionEnd = Prefixes.WSE.namespace + '/SubscriptionEnd'
    Unsubscribe = Prefixes.WSE.namespace + '/Unsubscribe'
    UnsubscribeResponse = Prefixes.WSE.namespace + '/UnsubscribeResponse'
    Renew = Prefixes.WSE.namespace + '/Renew'
    RenewResponse = Prefixes.WSE.namespace + '/RenewResponse'
    GetStatus = Prefixes.WSE.namespace + '/GetStatus'
    GetStatusResponse = Prefixes.WSE.namespace + '/GetStatusResponse'
