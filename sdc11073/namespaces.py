''' A helper for xml name space handling'''
from lxml import etree as etree_
from _functools import partial
from collections import namedtuple

_Prefix_Namespace_Tuple = namedtuple('_Prefix_Namespace_Tuple', 'prefix namespace')

from enum import Enum

# these are internally used namespaces, they are not all identical the ones that are used in sdc.
# it is in the responsibility of the sdc definitions class(es) to convert between internal and external namespaces.
# Originally this abstraction was needed, because during development of the sdc standard the namespaces changed.
# Although the standard is meanwhile final, this abstraction might again be needed if in the future a new revision
# of the standard appears.
class Prefix_Namespace(_Prefix_Namespace_Tuple, Enum):
    MSG = _Prefix_Namespace_Tuple('msg', "__BICEPS_MessageModel__")
    PM = _Prefix_Namespace_Tuple('dom', "__BICEPS_ParticipantModel__")
    MDPWS = _Prefix_Namespace_Tuple('mdpws', "__MDPWS__")
    EXT = _Prefix_Namespace_Tuple('ext', "__ExtensionPoint__")
    SDC = _Prefix_Namespace_Tuple('sdc', 'http://standards.ieee.org/downloads/11073/11073-20701-2018')
    WSE = _Prefix_Namespace_Tuple('wse', 'http://schemas.xmlsoap.org/ws/2004/08/eventing')
    XSD = _Prefix_Namespace_Tuple('xsd', 'http://www.w3.org/2001/XMLSchema')
    XSI = _Prefix_Namespace_Tuple('xsi', 'http://www.w3.org/2001/XMLSchema-instance')
    WSA = _Prefix_Namespace_Tuple('wsa', 'http://www.w3.org/2005/08/addressing')
    WSX = _Prefix_Namespace_Tuple('wsx', 'http://schemas.xmlsoap.org/ws/2004/09/mex') # Meta Data Exchange
    DPWS = _Prefix_Namespace_Tuple('dpws', 'http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01')
    WSD = _Prefix_Namespace_Tuple('wsd', 'http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01')
    S12 = _Prefix_Namespace_Tuple('s12', 'http://www.w3.org/2003/05/soap-envelope')
    XML = _Prefix_Namespace_Tuple('xml', 'http://www.w3.org/XML/1998/namespace')
    WXF = _Prefix_Namespace_Tuple('wxf', 'http://schemas.xmlsoap.org/ws/2004/09/transfer') # ws-transfer
    WSDL = _Prefix_Namespace_Tuple('wsdl', 'http://schemas.xmlsoap.org/wsdl/')
    @staticmethod
    def partialMap(*prefix):
        '''
        :param prefix: Prefix_Namespace_Tuples
        :return: a dictionary with prefix as key, namespace as value
        '''
        return dict((v.prefix, v.namespace) for v in prefix)


# these are all namespaces used in sdc:
nsmap = dict((item.prefix, item.namespace) for item in Prefix_Namespace)

def _tagName(prefix_namespace_tuple, tagname):
    return etree_.QName(prefix_namespace_tuple.namespace, tagname)

msgTag = partial(_tagName, Prefix_Namespace.MSG) # a helper to make qualified names
domTag = partial(_tagName, Prefix_Namespace.PM) # a helper to make qualified names
extTag = partial(_tagName, Prefix_Namespace.EXT) # a helper to make qualified names
wseTag = partial(_tagName, Prefix_Namespace.WSE) # a helper to make qualified names
xsiTag = partial(_tagName, Prefix_Namespace.XSI) # a helper to make qualified names
wsaTag = partial(_tagName, Prefix_Namespace.WSA) # a helper to make qualified names
wsxTag = partial(_tagName, Prefix_Namespace.WSX) # a helper to make qualified names
dpwsTag = partial(_tagName, Prefix_Namespace.DPWS) # a helper to make qualified names
mdpwsTag = partial(_tagName, Prefix_Namespace.MDPWS) # a helper to make qualified names
siTag  = partial(_tagName, Prefix_Namespace.MDPWS) # a helper to make qualified names
wsdTag = partial(_tagName, Prefix_Namespace.WSD) # a helper to make qualified names
s12Tag = partial(_tagName, Prefix_Namespace.S12) # a helper to make qualified names
xmlTag = partial(_tagName, Prefix_Namespace.XML) # a helper to make qualified names


# some constants from ws-addressing
WSA_ANONYMOUS = Prefix_Namespace.WSA.namespace + '/anonymous'
WSA_NONE = Prefix_Namespace.WSA.namespace + '/none'
WSA_IS_REFERENCE_PARAMETER = '{' + Prefix_Namespace.WSA.namespace + '}IsReferenceParameter'


def docNameFromQName(qName, ns_map):
    """ returns the docprefix:name string, or only name (if default namespace is used) """
    prefixmap = dict ((v,k) for k, v in ns_map.items())
    prefix = prefixmap[qName.namespace]
    if prefix is None:
        return qName.localname
    else:
        return '{}:{}'.format(prefix, qName.localname)


class DocNamespaceHelper(object):
    def __init__(self):
        self._prefixmap = dict ((x.namespace, x.prefix) for x in Prefix_Namespace)

    def useDocPrefixes(self, document_nsmap):
        for prefix, _ns in document_nsmap.items():
            self._prefixmap[_ns] = prefix

    def _docPrefix(self, prefix_namespace_tuple):
        ''' returns the document prefix for nsmap prefix'''
        return self._prefixmap[prefix_namespace_tuple.namespace]

    def msgPrefix(self):
        return self._prefixmap[Prefix_Namespace.MSG.namespace]

    def domPrefix(self):
        return self._prefixmap[Prefix_Namespace.PM.namespace]

    def docName(self, myPrefix_or_namespace, name):
        ''' returns the docprefix:name string. '''
        prefix = self._docPrefix(myPrefix_or_namespace)
        if prefix is None:
            return name
        else:
            return '{}:{}'.format(prefix, name)

    def docNameFromQName(self, qName):
        """ returns the docprefix:name string, or only name (if default namespace is used) """
        prefix = self._prefixmap[qName.namespace]
        if prefix is None:
            return qName.localname
        else:
            return '{}:{}'.format(prefix, qName.localname)

    @property
    def docNssmap(self):
        return dict ((v,k) for k, v in self._prefixmap.items())

    def partialMap(self, *prefix):
        '''
        :param prefix: Prefix class members
        :return: a dictionary with prefix as key, namespace as value
        '''
        mynamespaces = []
        for p in prefix:
            if isinstance(p, _Prefix_Namespace_Tuple):
                mynamespaces.append(p.namespace)
            else:
                mynamespaces.append(nsmap[p])
        return dict ((v,k) for k, v in self._prefixmap.items() if k in mynamespaces)


QN_TYPE = xsiTag('type')   # frequently used QName, central definition


def txt2QName(text, doc_nsmap):
    elements = text.split(':')
    prefix = None if len(elements) == 1 else elements[0]
    name = elements[-1]
    try:
        return etree_.QName(doc_nsmap[prefix], name)
    except KeyError:
        raise KeyError('Cannot make QName for {}, prefix is not in nsmap: {}'.format(text, doc_nsmap.keys()))
