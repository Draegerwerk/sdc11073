#!/usr/bin/env python
from lxml.etree import ETCompatXMLParser, QName, Element, SubElement, tostring, fromstring

import random
import socket
import struct
import time
import uuid
import threading
import platform
import selectors
import re
from collections import deque
import traceback
import logging
import urllib
from urllib.parse import urlparse
import queue
from dataclasses import dataclass, field
from typing import Any

try:
    from sdc11073.netconn import getNetworkAdapterConfigs
except ImportError:
    def getNetworkAdapterConfigs():
        return []
try:
    from sdc11073.commlogg import getCommunicationLogger
except ImportError:
    class NullLogger(object):
        """ This is a dummy logger that does nothing."""

        def __getattr__(self, name):
            return self.do_nothing

        def do_nothing(self, *args, **kwargs):
            pass


    communicationLogger = NullLogger()


    def getCommunicationLogger():
        return communicationLogger

BUFFER_SIZE = 0xffff
APP_MAX_DELAY = 500  # miliseconds
DP_MAX_TIMEOUT = 5000  # 5 seconds

_NETWORK_ADDRESSES_CHECK_TIMEOUT = 5

MULTICAST_PORT = 3702
MULTICAST_IPV4_ADDRESS = "239.255.255.250"
MULTICAST_OUT_TTL = 15  # Time To Live for multicast_out

UNICAST_UDP_REPEAT = 2
UNICAST_UDP_MIN_DELAY = 50
UNICAST_UDP_MAX_DELAY = 250
UNICAST_UDP_UPPER_DELAY = 500

MULTICAST_UDP_REPEAT = 4
MULTICAST_UDP_MIN_DELAY = 50
MULTICAST_UDP_MAX_DELAY = 250
MULTICAST_UDP_UPPER_DELAY = 500

# pylint: disable=protected-access, redefined-outer-name, len-as-condition, attribute-defined-outside-init

# values  acc to
#  http://docs.oasis-open.org/ws-dd/discovery/1.1/wsdd-discovery-1.1-spec.html
NS_A = "http://www.w3.org/2005/08/addressing"  # ws-addressing
NS_D = 'http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01'  # ws-discovery
NS_S = "http://www.w3.org/2003/05/soap-envelope"  # "http://www.w3.org/2003/05/soap-envelope"
NS_DPWS = 'http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01'
ACTION_HELLO = NS_D + '/Hello'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/Hello"
ACTION_BYE = NS_D + '/Bye'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/Bye"
ACTION_PROBE = NS_D + '/Probe'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/Probe"
ACTION_PROBE_MATCH = NS_D + '/ProbeMatches'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/ProbeMatches"
ACTION_RESOLVE = NS_D + '/Resolve'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/Resolve"
ACTION_RESOLVE_MATCH = NS_D + '/ResolveMatches'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/ResolveMatches"

ADDRESS_ALL = "urn:docs-oasis-open-org:ws-dd:ns:discovery:2009:01"  # format acc to RFC 2141
WSA_ANONYMOUS = NS_A + '/anonymous'
MATCH_BY_LDAP = NS_D + '/ldap'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/ldap"
MATCH_BY_URI = NS_D + '/rfc3986'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/rfc3986"
MATCH_BY_UUID = NS_D + '/uuid'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/uuid"
MATCH_BY_STRCMP = NS_D + '/strcmp0'  # "http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01/strcmp0"

_IP_BLACKLIST = ('0.0.0.0', None)  # None can happen if an adapter does not have any IP address

# these time constants control the send loop
SEND_LOOP_IDLE_SLEEP = 0.1
SEND_LOOP_BUSY_SLEEP = 0.01


class WsaTag(QName):
    def __init__(self, localname):
        super().__init__(NS_A, localname)


class WsdTag(QName):
    def __init__(self, localname):
        super().__init__(NS_D, localname)


class S12Tag(QName):
    def __init__(self, localname):
        super().__init__(NS_S, localname)


_namespaces_map = {'wsd': NS_D, 'wsa': NS_A, 's12': NS_S, 'dpws': NS_DPWS}


def types_info(types):
    # helper for logging
    return [str(t) for t in types] if types else types


def _getNetworkAddrs():
    """
    :return: a list of strings
    """
    result = []
    interfaces = getNetworkAdapterConfigs()
    for interface in interfaces:
        if interface.ip not in _IP_BLACKLIST:
            result.append(interface.ip)
    return result


def _getPrefix(nsmap, ns):
    for prefix, namespace in nsmap.items():
        if namespace == ns:
            return prefix


class URI:

    def __init__(self, uri):
        i1 = uri.find(":")
        i2 = uri.find("@")
        self._scheme = uri[:i1]
        if i2 != -1:
            self._authority = uri[i1 + 1: i2]
            self._path = uri[i2 + 1:]
        else:
            self._authority = ""
            self._path = uri[i1 + 1:]

    def getScheme(self):
        return self._scheme

    def getAuthority(self):
        return self._authority

    def getPath(self):
        return self._path

    def getPathExQueryFragment(self):
        i = self._path.find("?")
        path = self.getPath()
        if i != -1:
            return path[:self._path.find("?")]
        else:
            return path


class Scope:

    def __init__(self, value, matchBy=None):
        self._matchBy = matchBy
        self._value = value

    def getMatchBy(self):
        return self._matchBy

    def getValue(self):
        return self._value

    def getQuotedValue(self):
        return self._value.replace(' ', '%20')

    def __repr__(self):
        if self.getMatchBy() is None or len(self.getMatchBy()) == 0:
            return self.getValue()
        else:
            return self.getMatchBy() + ":" + self.getValue()


class ProbeResolveMatch:

    def __init__(self, epr, types, scopes, xAddrs, metadataVersion):
        self._epr = epr
        self._types = types
        self._scopes = scopes
        self._xAddrs = xAddrs
        self._metadataVersion = metadataVersion

    def getEPR(self):
        return self._epr

    def getTypes(self):
        return self._types

    def getScopes(self):
        return self._scopes

    def getXAddrs(self):
        return self._xAddrs

    def getMetadataVersion(self):
        return self._metadataVersion

    def __repr__(self):
        return "ProbeResolveMatch(EPR:%s Types:%s Scopes:%s XAddrs:%s Metadata Version:%s)" % \
               (self.getEPR(), types_info(self.getTypes()),
                [str(s) for s in self.getScopes()],
                self.getXAddrs(),
                self.getMetadataVersion())


class SoapEnvelope:

    def __init__(self, messageID=None):
        self._action = ""
        self._messageId = messageID or uuid.uuid4().urn
        self._relatesTo = ""
        self._relationshipType = None
        self._to = ""
        self._replyTo = ""
        self._instanceId = ""
        self._sequenceId = ""
        self._messageNumber = ""
        self._epr = ""
        self._types = []
        self._scopes = []
        self._xAddrs = []
        self._metadataVersion = "1"
        self._probeResolveMatches = []

    def getAction(self):
        return self._action

    def setAction(self, action):
        self._action = action

    def getMessageId(self):
        return self._messageId

    def getRelatesTo(self):
        return self._relatesTo

    def setRelatesTo(self, relatesTo):
        self._relatesTo = relatesTo

    def getRelationshipType(self):
        return self._relationshipType

    def setRelationshipType(self, relationshipType):
        self._relationshipType = relationshipType

    def getTo(self):
        return self._to

    def setTo(self, to):
        self._to = to

    def getReplyTo(self):
        return self._replyTo

    def setReplyTo(self, replyTo):
        self._replyTo = replyTo

    def getInstanceId(self):
        return self._instanceId

    def setInstanceId(self, instanceId):
        self._instanceId = instanceId

    def getSequenceId(self):
        return self._sequenceId

    def setSequenceId(self, sequenceId):
        self._sequenceId = sequenceId

    def getEPR(self):
        return self._epr

    def setEPR(self, epr):
        self._epr = epr

    def getMessageNumber(self):
        return self._messageNumber

    def setMessageNumber(self, messageNumber):
        self._messageNumber = messageNumber

    def getTypes(self):
        return self._types

    def setTypes(self, types):
        self._types = types

    def getScopes(self):
        return self._scopes

    def setScopes(self, scopes):
        self._scopes = scopes

    def getXAddrs(self):
        return self._xAddrs

    def setXAddrs(self, xAddrs):
        self._xAddrs = xAddrs

    def getMetadataVersion(self):
        return self._metadataVersion

    def setMetadataVersion(self, metadataVersion):
        self._metadataVersion = metadataVersion

    def getProbeResolveMatches(self):
        return self._probeResolveMatches

    def setProbeResolveMatches(self, probeResolveMatches):
        self._probeResolveMatches = probeResolveMatches


def matchScope(src, target, matchBy):
    """ This implementation correctly handles "%2F" (== '/') encoded values"""
    if matchBy == "" or matchBy is None or matchBy == MATCH_BY_LDAP or matchBy == MATCH_BY_URI or matchBy == MATCH_BY_UUID:
        src = urllib.parse.urlsplit(src)
        target = urllib.parse.urlsplit(target)
        if src.scheme.lower() != target.scheme.lower():
            return False
        if src.netloc.lower() != target.netloc.lower():
            return False
        if src.path == target.path:
            return True
        srcPathElements = src.path.split('/')
        targetPathElements = target.path.split('/')
        srcPathElements = [urllib.parse.unquote(elem) for elem in srcPathElements]
        targetPathElements = [urllib.parse.unquote(elem) for elem in targetPathElements]
        if len(srcPathElements) > len(targetPathElements):
            return False
        for i, elem in enumerate(srcPathElements):
            if targetPathElements[i] != elem:
                return False
        return True
    elif matchBy == MATCH_BY_STRCMP:
        return src == target
    else:
        return False


def matchType(type1, type2):
    return type1.namespace == type2.namespace and type1.localname == type2.localname


_ascii_letters = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ'


def getRandomStr():
    return "".join([random.choice(_ascii_letters) for x in range(10)])


def _parseTypes(parentNode):
    types = []
    typesNode = parentNode.find('wsd:Types', _namespaces_map)
    if typesNode is not None:
        _types = [] if not typesNode.text else typesNode.text.split()
        for t in _types:
            elements = t.split(':')
            prefix = None if len(elements) == 1 else elements[0]  # None means default namespace
            localname = elements[-1]
            q = QName(typesNode.nsmap[prefix], localname)
            types.append(q)
    return types


def _parseScopes(parentNode):
    scopesNode = parentNode.find('wsd:Scopes', _namespaces_map)
    if scopesNode is not None:
        matchBy = scopesNode.attrib.get("MatchBy")
        scopes = [] if not scopesNode.text else scopesNode.text.split()
        return [Scope(item, matchBy) for item in scopes]
    else:
        return []


def _parseXAddrs(parentNode):
    xAddrNode = parentNode.find('wsd:XAddrs', _namespaces_map)
    if xAddrNode is not None:
        return [] if not xAddrNode.text else xAddrNode.text.split()
    else:
        return []


def _parseEpr(parentNode):
    """

    :param parentNode: direct parent of wsa:EndpointReference node
    :return: epr address
    """
    eprNode = parentNode.find('wsa:EndpointReference', _namespaces_map)
    if eprNode is not None:
        addressNode = eprNode.find('wsa:Address', _namespaces_map)
        return addressNode.text
    return ''


def _parseMetaDataVersion(parentNode):
    mdvNode = parentNode.find('wsd:MetadataVersion', _namespaces_map)
    if mdvNode is not None:
        return mdvNode.text
    return ''


def _parseAppSequence(headerNode, env):
    appSeqNode = headerNode.find('wsd:AppSequence', _namespaces_map)
    if appSeqNode is not None:
        env.setInstanceId(appSeqNode.attrib.get("InstanceId"))
        env.setSequenceId(appSeqNode.attrib.get("SequenceId"))
        env.setMessageNumber(appSeqNode.attrib.get("MessageNumber"))


def _parseRelatesTo(headerNode, env):
    relatesTo = headerNode.find('wsa:RelatesTo', _namespaces_map)
    if relatesTo is not None:
        env.setRelatesTo(relatesTo.text)
        rel_type = relatesTo.attrib.get('RelationshipType')
        if rel_type:
            env.setRelationshipType(rel_type)


def _parseReplyTo(headerNode, env):
    replyTo = headerNode.find('wsa:ReplyTo', _namespaces_map)
    if replyTo is not None:
        env.setReplyTo(replyTo.text)


def parseEnvelope(data, ipAddr, logger):
    parser = ETCompatXMLParser()
    try:
        dom = fromstring(data, parser=parser)
    except Exception as ex:
        logger.error('load error "%s" in "%s"', ex, data)
        return

    header = dom.find('s12:Header', _namespaces_map)
    body = dom.find('s12:Body', _namespaces_map)
    if header is None or body is None:
        logger.error('received message from {} is not a soap message: {}', ipAddr, data)
        return None

    msgNode = body[0]

    msgId = header.find('wsa:MessageID', _namespaces_map)
    msgId = None if msgId is None else msgId.text
    soapAction = header.find('wsa:Action', _namespaces_map)
    if soapAction is None:
        # this is something else, ignore it
        return

    soapAction = soapAction.text

    env = SoapEnvelope(msgId)
    env.setAction(soapAction)

    to = header.find('wsa:To', _namespaces_map)
    if to is not None:
        env.setTo(to.text)

    # parse action specific data
    try:
        if soapAction == ACTION_PROBE:
            _parseReplyTo(header, env)
            env.getTypes().extend(_parseTypes(msgNode))
            env.getScopes().extend(_parseScopes(msgNode))
            return env
        elif soapAction == ACTION_PROBE_MATCH:
            _parseRelatesTo(header, env)
            _parseAppSequence(header, env)
            pmNodes = msgNode.findall('wsd:ProbeMatch', _namespaces_map)
            for node in pmNodes:
                epr = _parseEpr(node)
                types = _parseTypes(node)
                scopes = _parseScopes(node)
                xAddrs = _parseXAddrs(node)
                mdv = _parseMetaDataVersion(node)
                env.getProbeResolveMatches().append(ProbeResolveMatch(epr, types, scopes, xAddrs, mdv))
            return env
        elif soapAction == ACTION_RESOLVE:
            _parseReplyTo(header, env)
            env.setEPR(_parseEpr(msgNode))
            return env
        elif soapAction == ACTION_RESOLVE_MATCH:
            _parseRelatesTo(header, env)
            _parseAppSequence(header, env)
            resolveMatchNode = msgNode.find('wsd:ResolveMatch', _namespaces_map)
            if resolveMatchNode is not None:
                epr = _parseEpr(resolveMatchNode)
                types = _parseTypes(resolveMatchNode)
                scopes = _parseScopes(resolveMatchNode)
                xAddrs = _parseXAddrs(resolveMatchNode)
                mdv = _parseMetaDataVersion(resolveMatchNode)
                env.setProbeResolveMatches([ProbeResolveMatch(epr, types, scopes, xAddrs, mdv)])
            return env
        elif soapAction == ACTION_BYE:
            _parseAppSequence(header, env)
            env.setEPR(_parseEpr(msgNode))
            return env
        elif soapAction == ACTION_HELLO:
            _parseAppSequence(header, env)
            env.setEPR(_parseEpr(msgNode))
            env.getTypes().extend(_parseTypes(msgNode))
            env.getScopes().extend(_parseScopes(msgNode))
            env.setXAddrs(_parseXAddrs(msgNode))
            env.setMetadataVersion(_parseMetaDataVersion(msgNode))
            return env
    except:
        logger.error('Parse Error %s:', traceback.format_exc())
        logger.error('parsed data is from %r, data: %r:', ipAddr, data)
        return


class MessageCreator:
    """This class provides methods to create messages from SoapEnvelope instances."""
    def createMessage(self, env) -> bytes:
        action = env.getAction()
        if action == ACTION_PROBE:
            doc, header, body = self.createProbeMessage(env)
        elif action == ACTION_PROBE_MATCH:
            doc, header, body = self.createProbeMatchMessage(env)
        elif action == ACTION_RESOLVE:
            doc, header, body = self.createResolveMessage(env)
        elif action == ACTION_RESOLVE_MATCH:
            doc, header, body = self.createResolveMatchMessage(env)
        elif action == ACTION_HELLO:
            doc, header, body = self.createHelloMessage(env)
        elif action == ACTION_BYE:
            doc, header, body = self.createByeMessage(env)
        else:
            raise ValueError(f'unknown action {action}')
        return tostring(doc)

    def createProbeMessage(self, env):
        doc, header, body = self._createSkelSoapMessage(env.getAction(), env.getMessageId(),
                                                       to=env.getTo(),
                                                       replyTo=env.getReplyTo())
        probeEl = SubElement(body, WsdTag('Probe'))
        self._createTypeNodes(probeEl, env.getTypes())
        self._createScopeNodes(probeEl, env.getScopes())
        return doc, header, body

    def createProbeMatchMessage(self, env):
        doc, header, body = self._createSkelSoapMessage(env.getAction(), env.getMessageId(),
                                                       relatesTo=env.getRelatesTo(),
                                                       to=env.getTo(),
                                                       replyTo=env.getReplyTo(),
                                                       appSequence=self._mkAppSequenceDict(env))
        probeMatchesEl = SubElement(body, WsdTag('ProbeMatches'))

        probeMatches = env.getProbeResolveMatches()
        for probeMatch in probeMatches:
            probeMatchEl = SubElement(probeMatchesEl, WsdTag('ProbeMatch'))
            self._createEprNode(probeMatchEl, probeMatch.getEPR())
            self._createTypeNodes(probeMatchEl, probeMatch.getTypes())
            self._createScopeNodes(probeMatchEl, probeMatch.getScopes())
            self._createXAddrNodes(probeMatchEl, probeMatch.getXAddrs())
            self.mkSubElementWithText(probeMatchEl, WsdTag('MetadataVersion'), probeMatch.getMetadataVersion())
        return doc, header, body

    def createResolveMessage(self, env):
        doc, header, body = self._createSkelSoapMessage(env.getAction(), env.getMessageId(),
                                                        to=env.getTo(),
                                                        replyTo=env.getReplyTo())
        resolveEl = SubElement(body, WsdTag('Resolve'))
        self._createEprNode(resolveEl, env.getEPR())
        return doc, header, body

    def createResolveMatchMessage(self, env):
        doc, header, body = self._createSkelSoapMessage(env.getAction(),
                                                       env.getMessageId(),
                                                       relatesTo=env.getRelatesTo(),
                                                       to=env.getTo(),
                                                       appSequence=self._mkAppSequenceDict(env))
        resolveMatchesEl = SubElement(body, WsdTag('ResolveMatches'))
        if len(env.getProbeResolveMatches()) > 0:
            resolveMatch = env.getProbeResolveMatches()[0]
            resolveMatchEl = SubElement(resolveMatchesEl, WsdTag('ResolveMatch'))
            self._createEprNode(resolveMatchEl, resolveMatch.getEPR())
            self._createTypeNodes(resolveMatchEl, resolveMatch.getTypes())
            self._createScopeNodes(resolveMatchEl, resolveMatch.getScopes())
            self._createXAddrNodes(resolveMatchEl, resolveMatch.getXAddrs())
            self.mkSubElementWithText(resolveMatchEl, WsdTag('MetadataVersion'), resolveMatch.getMetadataVersion())
        return doc, header, body

    def createHelloMessage(self, env):
        doc, header, body = self._createSkelSoapMessage(env.getAction(), env.getMessageId(),
                                                       to=env.getTo(),
                                                       relatesTo=env.getRelatesTo(),
                                                       relatesToAttrib={"RelationshipType": "d:Suppression"},
                                                       appSequence=self._mkAppSequenceDict(env))
        helloEl = SubElement(body, WsdTag('Hello'))
        self._createEprNode(helloEl, env.getEPR())
        self._createTypeNodes(helloEl, env.getTypes())
        self._createScopeNodes(helloEl, env.getScopes())
        self._createXAddrNodes(helloEl, env.getXAddrs())
        self.mkSubElementWithText(helloEl, WsdTag('MetadataVersion'), env.getMetadataVersion())
        return doc, header, body

    def createByeMessage(self, env):
        doc, header, body = self._createSkelSoapMessage(env.getAction(), env.getMessageId(),
                                                       to=env.getTo(),
                                                       appSequence=self._mkAppSequenceDict(env))
        byeEl = SubElement(body, WsdTag('Bye'))
        self._createEprNode(byeEl, env.getEPR())
        return doc, header, body

    def _createSkelSoapMessage(self, soapAction, messageId,
                               relatesTo=None,
                               relatesToAttrib=None,
                               to=None,
                               replyTo=None,
                               appSequence=None):
        doc = Element(S12Tag('Envelope'),
                      nsmap=_namespaces_map)  # Prefix.partialMap(Prefix.S12, Prefix.WSA, Prefix.WSD, Prefix.DPWS))
        header = SubElement(doc, S12Tag('Header'))
        action = SubElement(header, WsaTag('Action'))
        action.text = soapAction
        body = SubElement(doc, S12Tag('Body'))

        self.mkSubElementWithText(header, WsaTag('MessageID'), messageId)
        if relatesTo:
            self.mkSubElementWithText(header, WsaTag('RelatesTo'), text=relatesTo, attrib=relatesToAttrib)
        if to:
            self.mkSubElementWithText(header, WsaTag('To'), to)
        if replyTo:
            self.mkSubElementWithText(header, WsaTag('ReplyTo'), replyTo)
        if appSequence:
            SubElement(header, WsdTag('AppSequence'), attrib=appSequence)
        return doc, header, body

    @staticmethod
    def _mkAppSequenceDict(env):
        return {"InstanceId": env.getInstanceId(),
                "MessageNumber": env.getMessageNumber()}

    def _createTypeNodes(self, parentNode, types):
        if types is not None and len(types) > 0:
            ns_map = {}
            typeList = []

            for i, _type in enumerate(types):
                ns, localname = _type.namespace, _type.localname
                prefix = _getPrefix(parentNode, ns)
                if prefix is None:
                    prefix = getRandomStr()
                    ns_map[prefix] = ns
                if i == 0:
                    # make namespace of first type the default namespace (so that we can test handling of this case)
                    ns_map[None] = ns
                    typeList.append(localname)
                else:
                    typeList.append(prefix + ":" + localname)

            typesString = " ".join(typeList)
            self.mkSubElementWithText(parentNode, WsdTag('Types'), typesString, nsmap=ns_map)

    def _createScopeNodes(self, parentNode, scopes):
        if scopes is not None and len(scopes) > 0:
            scopesString = " ".join([x.getQuotedValue() for x in scopes])
            self.mkSubElementWithText(parentNode, WsdTag('Scopes'), scopesString)

    def _createXAddrNodes(self, parentNode, xAddrs):
        if xAddrs is not len(xAddrs) > 0:
            addrString = " ".join([x for x in xAddrs])
            self.mkSubElementWithText(parentNode, WsdTag('XAddrs'), addrString)

    def _createEprNode(self, parentNode, epr):
        eprEl = SubElement(parentNode, WsaTag("EndpointReference"))
        self.mkSubElementWithText(eprEl, WsaTag('Address'), epr)

    @staticmethod
    def mkSubElementWithText(parentNode, qname, text, attrib=None, nsmap=None):
        elem = SubElement(parentNode, qname, attrib=attrib or {}, nsmap=nsmap or {})
        elem.text = text
        return elem


def extractSoapUdpAddressFromURI(uri):
    val = uri.getPathExQueryFragment().split(":")
    part1 = val[0][2:]
    part2 = None
    if val[1].count('/') > 0:
        part2 = int(val[1][:val[1].index('/')])
    else:
        part2 = int(val[1])
    addr = [part1, part2]
    return addr


class AddressMonitorThread(threading.Thread):
    """ This thread frequently checks the available Network adapters.
    Any change is reported vis wsd._networkAddressRemoved or wsd._networkAddressAdded
    """

    def __init__(self, wsd):
        self._addrs = set()
        self._wsd = wsd
        self._logger = logging.getLogger('sdc.discover.monitor')
        self._quitEvent = threading.Event()
        super().__init__(name='AddressMonitorThread')
        self.daemon = True
        self._updateAddrs()

    def _updateAddrs(self):
        addrs = set(_getNetworkAddrs())

        disappeared = self._addrs.difference(addrs)
        new = addrs.difference(self._addrs)

        for addr in disappeared:
            self._wsd._networkAddressRemoved(addr)

        for addr in new:
            try:
                self._wsd._networkAddressAdded(addr)
            except:
                self._logger.warning(traceback.format_exc())
        self._addrs = addrs

    def run(self):
        try:
            while not self._quitEvent.wait(_NETWORK_ADDRESSES_CHECK_TIMEOUT):
                self._updateAddrs()
        except Exception:
            self._logger.error('Unhandled Exception at thread runtime. Thread will abort! %s',
                               traceback.format_exc())
            raise

    def schedule_stop(self):
        """Schedule stopping the thread.
        Use join() to wait, until thread really has been stopped
        """
        self._quitEvent.set()


@dataclass(frozen=True)
class _SocketPair:
    multi_in: socket.socket
    multi_out_uni_in: socket.socket


class NetworkingThreadWindows:
    """ Has one thread for sending and one for receiving"""

    @dataclass(order=True)
    class _EnqueuedMessage:
        send_time: float
        msg: Any = field(compare=False)

    def __init__(self, observer, logger, multicast_port, message_creator):
        self._observer = observer
        self._logger = logger
        self.multicast_port = multicast_port
        self.message_creator = message_creator
        self._recvThread = None
        self._qread_thread = None
        self._sendThread = None
        self._quitRecvEvent = threading.Event()
        self._quitSendEvent = threading.Event()
        self._send_queue = queue.PriorityQueue(10000)
        self._read_queue = queue.Queue(10000)
        self._knownMessageIds = deque(maxlen=50)

        self._select_in = []
        self._full_selector = selectors.DefaultSelector()
        self._sockets_by_address = {}
        self._sockets_by_address_lock = threading.RLock()
        self._uni_out_socket = None

    def _register(self, sock):
        self._select_in.append(sock)
        self._full_selector.register(sock, selectors.EVENT_READ)

    def _unregister(self, sock):
        self._select_in.remove(sock)
        self._full_selector.unregister(sock)

    @staticmethod
    def _makeMreq(addr):
        return struct.pack("4s4s", socket.inet_aton(MULTICAST_IPV4_ADDRESS), socket.inet_aton(addr))

    @staticmethod
    def _createMulticastOutSocket(addr):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, MULTICAST_OUT_TTL)
        if addr is None:
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.INADDR_ANY)
        else:
            _addr = socket.inet_aton(addr)
            sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, _addr)
        return sock

    @staticmethod
    def _createMulticastInSocket(addr, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((addr, port))
        sock.setblocking(False)
        return sock

    def addSourceAddr(self, addr):
        """None means 'system default'"""
        multicast_in_sock = self._createMulticastInSocket(addr, self.multicast_port)
        try:
            multicast_in_sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, self._makeMreq(addr))
        except socket.error:  # if 1 interface has more than 1 address, exception is raised for the second
            print(traceback.format_exc())
            pass
        multicast_out_sock = self._createMulticastOutSocket(addr)
        with self._sockets_by_address_lock:
            self._register(multicast_out_sock)
            self._register(multicast_in_sock)
            self._sockets_by_address[addr] = _SocketPair(multicast_in_sock, multicast_out_sock)

    def removeSourceAddr(self, addr):
        sock_pair = self._sockets_by_address.get(addr)
        if sock_pair:
            with self._sockets_by_address_lock:
                for sock in (sock_pair.multi_in, sock_pair.multi_out_uni_in):
                    self._unregister(sock)
                    sock.close()
                del self._sockets_by_address[addr]

    def addUnicastMessage(self, env, addr, port, initialDelay=0):
        msg = Message(env, addr, port, Message.UNICAST, initialDelay)
        self._logger.debug(
            'addUnicastMessage: adding message Id %s. delay=%.2f', env.getMessageId(), initialDelay)
        self._repeated_enqueue_msg(msg, initialDelay, UNICAST_UDP_REPEAT, UNICAST_UDP_MIN_DELAY,
                                   UNICAST_UDP_MAX_DELAY, UNICAST_UDP_UPPER_DELAY)

    def addMulticastMessage(self, env, addr, port, initialDelay=0):
        msg = Message(env, addr, port, Message.MULTICAST, initialDelay)
        self._logger.debug(
            'addMulticastMessage: adding message Id %s. delay=%.2f', env.getMessageId(), initialDelay)
        self._repeated_enqueue_msg(msg, initialDelay, MULTICAST_UDP_REPEAT, MULTICAST_UDP_MIN_DELAY,
                                   MULTICAST_UDP_MAX_DELAY, MULTICAST_UDP_UPPER_DELAY)

    def _repeated_enqueue_msg(self, msg, initial_delay_ms, repeat, min_delay_ms, max_delay_ms, upper_delay_ms):
        if not self._quitSendEvent.is_set():
            next_send = time.time() + initial_delay_ms / 1000.0
            dt = random.randrange(min_delay_ms, max_delay_ms) / 1000.0  # millisec -> seconds
            self._send_queue.put(self._EnqueuedMessage(next_send, msg))
            for _ in range(repeat):
                next_send += dt
                self._send_queue.put(self._EnqueuedMessage(next_send, msg))
                dt = min(dt * 2, upper_delay_ms)

    def _run_send(self):
        """send-loop"""
        while not self._quitSendEvent.is_set() or not self._send_queue.empty():
            if self._send_queue.empty():
                time.sleep(SEND_LOOP_IDLE_SLEEP)  # nothing to do currently
            else:
                if self._send_queue.queue[0].send_time <= time.time():
                    enqueued_msg = self._send_queue.get()
                    self._sendMsg(enqueued_msg.msg)
                else:
                    time.sleep(SEND_LOOP_BUSY_SLEEP)  # this creates a 10ms raster for sending, but that is good enough

    def _run_recv(self):
        """ run by thread"""
        while not self._quitRecvEvent.is_set():
            if len(self._sockets_by_address) == 0:
                # avoid errors while no sockets are registered
                time.sleep(0.1)
                continue
            try:
                self._recv_messages()
            except:
                if not self._quitRecvEvent.is_set():  # only log error if it does not happen during stop
                    self._logger.error('_run_recv:%s', traceback.format_exc())

    def isFromMySocket(self, addr):
        with self._sockets_by_address_lock:
            for ip_addr, sock_pair in self._sockets_by_address.items():
                if addr[0] == ip_addr:
                    try:
                        sock_name = sock_pair.multi_out_uni_in.getsockname()
                        if addr[1] == sock_name[1]:  # compare ports
                            return True
                    except OSError:  # port is not opened
                        continue
        return False

    def _recv_messages(self):
        """For performance reasons this thread only writes to a queue, no parsing etc."""
        for key, events in self._full_selector.select(timeout=0.1):
            sock = key.fileobj
            try:
                data, addr = sock.recvfrom(BUFFER_SIZE)
            except socket.error as e:
                self._logger.warning('socket read error %s', e)
                time.sleep(0.01)
                continue
            if self.isFromMySocket(addr):
                continue
            self._add_to_recv_queue(addr, data)

    def _add_to_recv_queue(self, addr, data):
        # method is needed for testing
        self._read_queue.put((addr, data))

    def _run_q_read(self):
        """Read from internal queue and process message"""
        while not self._quitRecvEvent.is_set():
            try:
                try:
                    incoming = self._read_queue.get(timeout=0.1)
                except queue.Empty:
                    pass
                else:
                    addr, data = incoming
                    getCommunicationLogger().logDiscoveryMsgIn(addr[0], data)

                    env = parseEnvelope(data, addr[0], self._logger)
                    if env is None:  # fault or failed to parse
                        continue

                    mid = env.getMessageId()
                    if mid in self._knownMessageIds:
                        self._logger.debug('message Id %s already known. This is a duplicate receive, ignoring.', mid)
                        continue
                    else:
                        self._knownMessageIds.appendleft(mid)
                    self._observer.envReceived(env, addr)
            except:
                self._logger.error('unexpected error in queue read thread: %s', traceback.format_exc())
        self._logger.info('queue read thread terminates')

    def _sendMsg(self, msg):
        action = msg._env.getAction().split('/')[-1]  # only last part
        if action in ('ResolveMatches', 'ProbeMatches'):
            self._logger.debug('_sendMsg: sending %s %s to %s ProbeResolveMatches=%r, epr=%s, msgNo=%r',
                               action,
                               msg.msgType(),
                               msg.getAddr(),
                               msg._env.getProbeResolveMatches(),
                               msg._env.getEPR(),
                               msg._env._messageNumber
                               )
        elif action == 'Probe':
            self._logger.debug('_sendMsg: sending %s %s to %s types=%s scopes=%r',
                               action,
                               msg.msgType(),
                               msg.getAddr(),
                               types_info(msg._env.getTypes()),
                               msg._env.getScopes(),
                               )
        else:
            self._logger.debug('_sendMsg: sending %s %s to %s xaddr=%r, epr=%s, msgNo=%r',
                               action,
                               msg.msgType(),
                               msg.getAddr(),
                               msg._env.getXAddrs(),
                               msg._env.getEPR(),
                               msg._env._messageNumber
                               )

        data = self.message_creator.createMessage(msg.getEnv())

        if msg.msgType() == Message.UNICAST:
            getCommunicationLogger().logDiscoveryMsgOut(msg.getAddr(), data)
            self._uniOutSocket.sendto(data, (msg.getAddr(), msg.getPort()))
        else:
            getCommunicationLogger().logBroadCastMsgOut(data)
            with self._sockets_by_address_lock:
                for sock_pair in self._sockets_by_address.values():
                    sock_pair.multi_out_uni_in.sendto(data, (msg.getAddr(), msg.getPort()))

    def start(self):
        self._logger.debug('%s: starting ', self.__class__.__name__)
        self._uniOutSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        self._recvThread = threading.Thread(target=self._run_recv, name='wsd.recvThread')
        self._qread_thread = threading.Thread(target=self._run_q_read, name='wsd.qreadThread')
        self._sendThread = threading.Thread(target=self._run_send, name='wsd.sendThread')
        self._recvThread.daemon = True
        self._qread_thread.daemon = True
        self._sendThread.daemon = True
        self._recvThread.start()
        self._qread_thread.start()
        self._sendThread.start()

    def schedule_stop(self):
        """Schedule stopping the thread.
        Use join() to wait, until thread really has been stopped
        """
        self._logger.debug('%s: schedule_stop ', self.__class__.__name__)
        self._quitRecvEvent.set()
        self._quitSendEvent.set()

    def join(self):
        self._logger.debug('%s: join... ', self.__class__.__name__)
        self._recvThread.join(1)
        self._qread_thread.join(1)
        self._sendThread.join(10)
        self._recvThread = None
        self._qread_thread = None
        self._sendThread = None
        for sock in self._select_in:
            sock.close()
        self._uniOutSocket.close()
        self._full_selector.close()
        self._logger.debug('%s: ... join done', self.__class__.__name__)

    def getActiveAddresses(self):
        with self._sockets_by_address_lock:
            return list(self._sockets_by_address.keys())


@dataclass(frozen=True)
class _Sockets:
    multi_in: socket.socket
    uni_in: socket.socket
    multi_out_uni_in: socket.socket


class NetworkingThreadPosix(NetworkingThreadWindows):

    @staticmethod
    def _createMulticastInSocket(addr, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((MULTICAST_IPV4_ADDRESS, port))
        sock.setblocking(False)
        mreq = struct.pack("4s4s", socket.inet_aton(MULTICAST_IPV4_ADDRESS), socket.inet_aton(addr))
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        return sock

    @staticmethod
    def _createUnicastInSocket(addr, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((addr, port))
        sock.setblocking(False)
        return sock

    def addSourceAddr(self, addr):
        """None means 'system default'"""
        try:
            multicast_in_sock = self._createMulticastInSocket(addr, self.multicast_port)
        except socket.error:  # if 1 interface has more than 1 address, exception is raised for the second
            print(traceback.format_exc())
            pass
        unicast_in_sock = self._createUnicastInSocket(addr,
                                                      self.multicast_port)  # allows handling of unicast messages on self.multicast_port
        multicast_out_sock = self._createMulticastOutSocket(addr)

        with self._sockets_by_address_lock:
            self._register(multicast_out_sock)
            self._register(unicast_in_sock)
            self._register(multicast_in_sock)
            self._sockets_by_address[addr] = _Sockets(multicast_in_sock, unicast_in_sock, multicast_out_sock)

    def removeSourceAddr(self, addr):
        sockets = self._sockets_by_address.get(addr)
        if sockets:
            with self._sockets_by_address_lock:
                for sock in (sockets.multi_in, sockets.uni_in, sockets.multi_out_uni_in):
                    self._unregister(sock)
                    sock.close()
                del self._sockets_by_address[addr]


class Message:
    MULTICAST = 'multicast'
    UNICAST = 'unicast'

    def __init__(self, env, addr, port, msgType, initialDelay=0):
        """msgType shall be Message.MULTICAST or Message.UNICAST"""
        self._env = env
        self._addr = addr
        self._port = port
        self._msgType = msgType

    def getEnv(self):
        return self._env

    def getAddr(self):
        return self._addr

    def getPort(self):
        return self._port

    def msgType(self):
        return self._msgType


class Service:
    def __init__(self, types, scopes, xAddrs, epr, instanceId, metadata_version=1):
        self._types = types
        self._scopes = scopes
        self._xAddrs = xAddrs
        self._epr = epr
        self._instanceId = instanceId
        self._messageNumber = 0
        self._metadataVersion = metadata_version

    def getTypes(self):
        return self._types

    def setTypes(self, types):
        self._types = types

    def getScopes(self):
        return self._scopes

    def setScopes(self, scopes):
        self._scopes = scopes

    def getXAddrs(self):
        ret = []
        ipAddrs = None
        for xAddr in self._xAddrs:
            if '{ip}' in xAddr:
                if ipAddrs is None:
                    ipAddrs = _getNetworkAddrs()
                for ipAddr in ipAddrs:
                    if ipAddr not in _IP_BLACKLIST:
                        ret.append(xAddr.format(ip=ipAddr))
            else:
                ret.append(xAddr)
        return ret

    def setXAddrs(self, xAddrs):
        self._xAddrs = xAddrs

    def getEPR(self):
        return self._epr

    def setEPR(self, epr):
        self._epr = epr

    def getInstanceId(self):
        return self._instanceId

    def setInstanceId(self, instanceId):
        self._instanceId = instanceId

    def getMessageNumber(self):
        return self._messageNumber

    def setMessageNumber(self, messageNumber):
        self._messageNumber = messageNumber

    def getMetadataVersion(self):
        return self._metadataVersion

    def setMetadataVersion(self, metadataVersion):
        self._metadataVersion = metadataVersion

    def incrementMetadataVersion(self):
        self._metadataVersion = self._metadataVersion + 1

    def incrementMessageNumber(self):
        self._messageNumber = self._messageNumber + 1

    def isLocatedOn(self, *ipaddresses):
        """
        :param ipaddresses: ip addresses, lists of strings or strings
        """
        my_addresses = []
        for i in ipaddresses:
            if isinstance(i, str):
                my_addresses.append(i)
            else:
                my_addresses.extend(i)
        for addr in self.getXAddrs():
            parsed = urllib.parse.urlsplit(addr)
            ip_addr = parsed.netloc.split(':')[0]
            if ip_addr in my_addresses:
                return True
        return False

    def __repr__(self):
        return 'Service epr={}, instanceId={} Xaddr={} scopes={} types={}'.format(self._epr, self._instanceId,
                                                                                  self._xAddrs,
                                                                                  ', '.join(
                                                                                      [str(x) for x in self._scopes]),
                                                                                  ', '.join(
                                                                                      [str(x) for x in self._types]))

    def __str__(self):
        return 'Service epr={}, instanceId={}\n   Xaddr={}\n   scopes={}\n   types={}'.format(self._epr,
                                                                                              self._instanceId,
                                                                                              self._xAddrs,
                                                                                              ', '.join([str(x) for x in
                                                                                                         self._scopes]),
                                                                                              ', '.join([str(x) for x in
                                                                                                         self._types]))


def _isTypeInList(ttype, types):
    for entry in types:
        if matchType(ttype, entry):
            return True
    return False


def _isScopeInList(scope, scopes):
    for entry in scopes:
        if matchScope(scope.getValue(), entry.getValue(), scope.getMatchBy()):
            return True
    return False


def _matchesFilter(service, types, scopes, logger=None):
    if types is not None:
        srv_ty = service.getTypes()
        for ttype in types:
            if not _isTypeInList(ttype, srv_ty):
                if logger:
                    logger.debug('types not matching: {} is not in types list {}'.format(ttype, srv_ty))
                return False
        if logger:
            logger.debug('matching types')
    if scopes is not None:
        srv_sc = service.getScopes()
        for scope in scopes:
            if not _isScopeInList(scope, srv_sc):
                if logger:
                    logger.debug('scope not matching: {} is not in scopes list {}'.format(scope, srv_sc))
                return False
        if logger:
            logger.debug('matching scopes')
    return True


def filterServices(services, types, scopes, logger=None):
    return [service for service in services if _matchesFilter(service, types, scopes, logger)]


class WSDiscoveryBase():
    """
    UDP based discovery.
    """
    # these flags control which data is included in ProbeResponse messages.
    PROBEMATCH_EPR = True
    PROBEMATCH_TYPES = True
    PROBEMATCH_SCOPES = True
    PROBEMATCH_XADDRS = True

    SoapEnvelopeCls = SoapEnvelope
    ServiceCls = Service
    MessageCreatorCls = MessageCreator

    def __init__(self, logger=None, multicast_port=None):
        """
        :param logger: use this logger. if None a logger 'sdc.discover' is created.
        :param multicast_port: a port number; if None, the default MULTICAST_PORT is used
        """
        self._logger = logger or logging.getLogger('sdc.discover')
        self.multicast_port = multicast_port or MULTICAST_PORT
        self._networkingThread = None
        self._addrsMonitorThread = None
        self._serverStarted = False
        self._remoteServices = {}
        self._localServices = {}

        self._dpActive = False  # True if discovery proxy detected (is not relevant in sdc context)
        self._dpAddr = None
        self._dpEPR = None

        self._remoteServiceProbeMatchCallback = None
        self._remoteServiceHelloCallback = None
        self._remoteServiceHelloCallbackTypesFilter = None
        self._remoteServiceHelloCallbackScopesFilter = None
        self._remoteServiceByeCallback = None
        self._remoteServiceResolveMatchCallback = None  # B.D.
        self._onProbeCallback = None
        random.seed(int(time.time() * 1000000))

    def setRemoteServiceProbeMatchCallback(self, cb):
        """Set callback, which will be called when a service was received via a ProbeMatch message.
        Service is passed as a parameter to the callback
        Set None to disable callback
        """
        self._remoteServiceProbeMatchCallback = cb

    def setRemoteServiceHelloCallback(self, cb, types=None, scopes=None):
        """Set callback, which will be called when new service appeared online
        and sent Hi message

        typesFilter and scopesFilter might be list of types and scopes.
        If filter is set, callback is called only for Hello messages,
        which match filter

        Set None to disable callback
        """
        self._remoteServiceHelloCallback = cb
        self._remoteServiceHelloCallbackTypesFilter = types
        self._remoteServiceHelloCallbackScopesFilter = scopes

    def setRemoteServiceByeCallback(self, cb):
        """Set callback, which will be called when a service goes offline and sent a Bye message
        Set None to disable callback
        """
        self._remoteServiceByeCallback = cb

    def setRemoteServiceResolveMatchCallback(self, cb):  # B.D.
        self._remoteServiceResolveMatchCallback = cb

    def setOnProbeCallback(self, cb):
        self._onProbeCallback = cb

    def _addRemoteService(self, service):
        epr = service.getEPR()
        if not epr:
            self._logger.info('service without epr, ignoring it! %r', service)
            return
        s = self._remoteServices.get(service.getEPR())
        if not s:
            self._remoteServices[service.getEPR()] = service
            self._logger.info('new remote %r', service)
        else:
            if service.getMetadataVersion() == s.getMetadataVersion():
                self._logger.debug('_addRemoteService: remote Service %s:\n    MetadataVersion: %d',
                                   service.getEPR(), service.getMetadataVersion())
                merged = []
                if len(service.getXAddrs()) > len(s.getXAddrs()):
                    s.setXAddrs(service.getXAddrs())
                    merged.append('XAddr={}'.format(service.getXAddrs()))

                if len(service.getScopes()) > len(s.getScopes()):
                    s.setScopes(service.getScopes())
                    merged.append('Scopes={}'.format(service.getScopes()))

                if len(service.getTypes()) > len(s.getTypes()):
                    s.setTypes(service.getTypes())
                    merged.append('Types={}'.format(service.getTypes()))
                if merged:
                    self._logger.info('merge from remote Service %s:\n      %r',
                                      service.getEPR(), '\n      '.join(merged))
            elif service.getMetadataVersion() > s.getMetadataVersion():
                self._logger.info('remote Service %s:\n    updated MetadataVersion\n      '
                                  'updated: %d\n      existing: %d',
                                  service.getEPR(), service.getMetadataVersion(), s.getMetadataVersion())
                self._remoteServices[service.getEPR()] = service
            else:
                self._logger.debug('_addRemoteService: remote Service %s:\n    outdated MetadataVersion\n      '
                                   'outdated: %d\n      existing: %d',
                                   service.getEPR(), service.getMetadataVersion(), s.getMetadataVersion())

    def _removeRemoteService(self, epr):
        if epr in self._remoteServices:
            del self._remoteServices[epr]

    def handleEnv(self, env, addr):
        act = env.getAction()
        self._logger.debug('handleEnv: received %s from %s', act.split('/')[-1], addr)
        if act == ACTION_PROBE_MATCH:
            for match in env.getProbeResolveMatches():
                service = self.ServiceCls(match.getTypes(), match.getScopes(), match.getXAddrs(), match.getEPR(),
                                          env.getInstanceId(), metadata_version=int(match.getMetadataVersion()))
                self._addRemoteService(service)
                if match.getXAddrs() is None or len(match.getXAddrs()) == 0:
                    self._logger.info('%s(%s) has no Xaddr, sending resolve message', match.getEPR(), addr)
                    self._sendResolve(match.getEPR())
                elif not match.getTypes():
                    self._logger.info('%s(%s) has no Types, sending resolve message', match.getEPR(), addr)
                    self._sendResolve(match.getEPR())
                elif not match.getScopes():
                    self._logger.info('%s(%s) has no Scopes, sending resolve message', match.getEPR(), addr)
                    self._sendResolve(match.getEPR())

                if self._remoteServiceProbeMatchCallback is not None:
                    self._remoteServiceProbeMatchCallback(addr, service)

        elif act == ACTION_RESOLVE_MATCH:
            for match in env.getProbeResolveMatches():
                service = self.ServiceCls(match.getTypes(), match.getScopes(), match.getXAddrs(), match.getEPR(),
                                          env.getInstanceId(), metadata_version=int(match.getMetadataVersion()))
                self._addRemoteService(service)
                if self._remoteServiceResolveMatchCallback is not None:
                    self._remoteServiceResolveMatchCallback(service)

        elif act == ACTION_PROBE:
            services = filterServices(self._localServices.values(), env.getTypes(), env.getScopes())
            if services:
                self._sendProbeMatch(services, env.getMessageId(), addr)
            if self._onProbeCallback is not None:
                self._onProbeCallback(addr, env)

        elif act == ACTION_RESOLVE:
            if env.getEPR() in self._localServices:
                service = self._localServices[env.getEPR()]
                self._sendResolveMatch(service, env.getMessageId(), addr)

        elif act == ACTION_HELLO:
            # check if it is from a discovery proxy
            rt = env.getRelationshipType()
            if rt is not None and rt.localname == "Suppression" and rt.namespace == NS_D:
                xAddr = env.getXAddrs()[0]
                # only support 'soap.udp'
                if xAddr.startswith("soap.udp:"):
                    self._dpActive = True
                    self._dpAddr = extractSoapUdpAddressFromURI(URI(xAddr))
                    self._dpEPR = env.getEPR()

            service = self.ServiceCls(env.getTypes(), env.getScopes(), env.getXAddrs(), env.getEPR(),
                                      env.getInstanceId(),
                                      metadata_version=int(env.getMetadataVersion()))
            self._addRemoteService(service)
            if not env.getXAddrs():  # B.D.
                self._logger.debug('%s(%s) has no Xaddr, sending resolve message', env.getEPR(), addr)
                self._sendResolve(env.getEPR())
            if self._remoteServiceHelloCallback is not None:
                if _matchesFilter(service,
                                  self._remoteServiceHelloCallbackTypesFilter,
                                  self._remoteServiceHelloCallbackScopesFilter):
                    self._remoteServiceHelloCallback(addr, service)

        elif act == ACTION_BYE:
            # if the bye is from discovery proxy... revert back to multicasting
            if self._dpActive and self._dpEPR == env.getEPR():
                self._dpActive = False
                self._dpAddr = None
                self._dpEPR = None

            self._removeRemoteService(env.getEPR())
            if self._remoteServiceByeCallback is not None:
                self._remoteServiceByeCallback(addr, env.getEPR())
        else:
            self._logger.info('unknown action %s', act)

    def envReceived(self, env, addr):
        self.handleEnv(env, addr)

    def _mkResolveMatchEnvelope(self, service, relatesTo):
        env = self.SoapEnvelopeCls()
        env.setAction(ACTION_RESOLVE_MATCH)
        env.setTo(WSA_ANONYMOUS)
        env.setInstanceId(str(service.getInstanceId()))
        env.setMessageNumber(str(service.getMessageNumber()))
        env.setRelatesTo(relatesTo)
        env.setProbeResolveMatches([ProbeResolveMatch(service.getEPR(), service.getTypes(), service.getScopes(),
                                                      service.getXAddrs(), str(service.getMetadataVersion()))])
        return env

    def _sendResolveMatch(self, service, relatesTo, addr):
        self._logger.info('sending resolve match to %s', addr)
        service.incrementMessageNumber()
        env = self._mkResolveMatchEnvelope(service, relatesTo)
        self._networkingThread.addUnicastMessage(env, addr[0], addr[1])

    def _mkProbeMatchEnvelope(self, service, relatesTo, msgNumber):
        env = self.SoapEnvelopeCls()
        env.setAction(ACTION_PROBE_MATCH)
        env.setTo(WSA_ANONYMOUS)
        env.setInstanceId(self.generate_instance_id())
        env.setMessageNumber(str(msgNumber))
        env.setRelatesTo(relatesTo)

        # add values to ProbeResponse acc. to flags
        epr = service.getEPR() if self.PROBEMATCH_EPR else ''
        types = service.getTypes() if self.PROBEMATCH_TYPES else []
        scopes = service.getScopes() if self.PROBEMATCH_SCOPES else []
        xaddrs = service.getXAddrs() if self.PROBEMATCH_XADDRS else []
        env.setProbeResolveMatches([ProbeResolveMatch(epr, types, scopes, xaddrs,
                                                      str(service.getMetadataVersion()))])
        return env

    def _sendProbeMatch(self, services, relatesTo, addr):
        self._logger.info('sending probe match to %s for %d services', addr, len(services))
        msgNumber = 1
        # send one match response for every service, dpws explorer can't handle telegram otherwise if too many devices reported
        for service in services:
            self._logger.debug('sending probe match 1')
            env = self._mkProbeMatchEnvelope(service, relatesTo, msgNumber)
            self._logger.debug('sending probe match 2')
            self._networkingThread.addUnicastMessage(env, addr[0], addr[1], random.randint(0, APP_MAX_DELAY))

    def _mkProbeEnvelope(self, types, scopes):
        env = self.SoapEnvelopeCls()
        env.setAction(ACTION_PROBE)
        env.setTo(ADDRESS_ALL)
        env.setTypes(types)
        env.setScopes(scopes)
        return env

    def _sendProbe(self, types=None, scopes=None):
        self._logger.debug('sending probe types=%r scopes=%r', types_info(types), scopes)
        env = self._mkProbeEnvelope(types, scopes)
        if self._dpActive:
            self._networkingThread.addUnicastMessage(env, self._dpAddr[0], self._dpAddr[1])
        else:
            self._networkingThread.addMulticastMessage(env, MULTICAST_IPV4_ADDRESS, self.multicast_port)

    def _mkResolveEnvelope(self, epr):
        env = self.SoapEnvelopeCls()
        env.setAction(ACTION_RESOLVE)
        env.setTo(ADDRESS_ALL)
        env.setEPR(epr)
        return env

    def _sendResolve(self, epr):
        self._logger.debug('sending resolve on %s', epr)
        env = self._mkResolveEnvelope(epr)
        if self._dpActive:
            self._networkingThread.addUnicastMessage(env, self._dpAddr[0], self._dpAddr[1])
        else:
            self._networkingThread.addMulticastMessage(env, MULTICAST_IPV4_ADDRESS, self.multicast_port)

    def _mkHelloEnvelope(self, service):
        env = self.SoapEnvelopeCls()
        env.setAction(ACTION_HELLO)
        env.setTo(ADDRESS_ALL)
        env.setInstanceId(str(service.getInstanceId()))
        env.setMessageNumber(str(service.getMessageNumber()))
        env.setTypes(service.getTypes())
        env.setScopes(service.getScopes())
        env.setXAddrs(service.getXAddrs())
        env.setEPR(service.getEPR())
        env.setMetadataVersion(str(service.getMetadataVersion()))
        return env

    def _sendHello(self, service):
        self._logger.info('sending hello on %s', service)
        service.incrementMessageNumber()
        env = self._mkHelloEnvelope(service)
        self._networkingThread.addMulticastMessage(env, MULTICAST_IPV4_ADDRESS, self.multicast_port,
                                                   random.randint(0, APP_MAX_DELAY))

    def _mkByeEnvelope(self, service):
        env = self.SoapEnvelopeCls()
        env.setAction(ACTION_BYE)
        env.setTo(ADDRESS_ALL)
        env.setInstanceId(str(service.getInstanceId()))
        env.setMessageNumber(str(service.getMessageNumber()))
        env.setEPR(service.getEPR())
        return env

    def _sendBye(self, service):
        self._logger.debug('sending bye on %s', service)
        env = self._mkByeEnvelope(service)
        service.incrementMessageNumber()
        self._networkingThread.addMulticastMessage(env, MULTICAST_IPV4_ADDRESS, self.multicast_port)

    def start(self):
        """start the discovery server - should be called before using other functions"""
        if not self._serverStarted:
            self._startThreads()
            self._serverStarted = True

    def stop(self):
        """cleans up and stops the discovery server"""
        if self._serverStarted:
            self.clearRemoteServices()
            self.clearLocalServices()

            self._stopThreads()
            self._serverStarted = False

    def _isAcceptedAddress(self, addr):  # pylint: disable=unused-argument
        """ accept any interface. Overwritten in derived classes."""
        return True

    def _networkAddressAdded(self, addr):
        if not self._isAcceptedAddress(addr):
            self._logger.debug('network Address ignored: %s', addr)
            return

        self._logger.debug('network Address Add: %s', addr)
        try:
            self._networkingThread.addSourceAddr(addr)
            for service in self._localServices.values():
                self._sendHello(service)
        except:
            self._logger.warning('error in network Address "%s" Added: %s', addr, traceback.format_exc())

    def _networkAddressRemoved(self, addr):
        self._logger.debug('network Address removed %s', addr)
        self._networkingThread.removeSourceAddr(addr)

    def _startThreads(self):
        if self._networkingThread is not None:
            return
        message_creator = self.MessageCreatorCls()
        if platform.system() != 'Windows':
            self._networkingThread = NetworkingThreadPosix(self, self._logger, self.multicast_port, message_creator)
        else:
            self._networkingThread = NetworkingThreadWindows(self, self._logger, self.multicast_port, message_creator)
        self._networkingThread.start()

        self._addrsMonitorThread = AddressMonitorThread(self)
        self._addrsMonitorThread.start()

    def _stopThreads(self):
        if self._networkingThread is None:
            return

        self._networkingThread.schedule_stop()
        self._addrsMonitorThread.schedule_stop()

        self._networkingThread.join()
        self._addrsMonitorThread.join()

        self._networkingThread = None
        self._addrsMonitorThread = None

    def clearRemoteServices(self):
        """clears remotely discovered services"""
        self._remoteServices.clear()

    def clearLocalServices(self):
        """send Bye messages for the services and remove them"""
        for service in self._localServices.values():
            self._sendBye(service)
        self._localServices.clear()

    def searchServices(self, types=None, scopes=None, timeout=5, repeatProbeInterval=3):
        """search for services given the TYPES and SCOPES in a given timeout
        :param repeatProbeInterval: send another probe message after x seconds"""
        if not self._serverStarted:
            raise Exception("Server not started")

        start = time.monotonic()
        end = start + timeout
        now = time.monotonic()
        while now < end:
            self._sendProbe(types, scopes)
            if now + repeatProbeInterval <= end:
                time.sleep(repeatProbeInterval)
            elif now < end:
                time.sleep(end - now)
            now = time.monotonic()
        return filterServices(self._remoteServices.values(), types, scopes)

    def searchMultipleTypes(self, typesList, scopes=None, timeout=10, repeatProbeInterval=3):
        """search for services given the list of TYPES and SCOPES in a given timeout.
        It returns services that match at least one of the types (OR condition).
        Can be used to search for devices that support Biceps Draft6 and Final with one search.
        :param repeatProbeInterval: send another probe message after x seconds"""
        if not self._serverStarted:
            raise Exception("Server not started")

        start = time.monotonic()
        end = start + timeout
        now = time.monotonic()
        while now < end:
            for t in typesList:
                self._sendProbe(t, scopes)
            now = time.monotonic()
            if now + repeatProbeInterval <= end:
                time.sleep(repeatProbeInterval)
            elif now < end:
                time.sleep(end - now)
        result = []
        for t in typesList:
            result.extend(filterServices(self._remoteServices.values(), t, scopes))
        return result

    def searchMedicalDeviceServicesinLocation(self, sdcLocation, timeout=3, bicepsVersion=None):
        if bicepsVersion is None:
            types = _FallbackMedicalDeviceTypesFilter
        else:
            types = bicepsVersion.MedicalDeviceTypesFilter
        services = self.searchServices(types=types, timeout=timeout)
        return sdcLocation.matchingServices(services)

    def publishService(self, epr, types, scopes, xAddrs):
        service = self._mkService(epr, types, scopes, xAddrs)
        self._sendHello(service)

    def _mkService(self, epr, types, scopes, xAddrs):
        """Publish a service with the given TYPES, SCOPES and XAddrs (service addresses)

        if xAddrs contains item, which includes {ip} pattern, one item per IP addres will be sent
        """
        if not self._serverStarted:
            raise Exception("Server not started")

        instanceId = self.generate_instance_id()
        metadata_version = self._localServices[epr].getMetadataVersion() + 1 if epr in self._localServices else 1
        service = self.ServiceCls(types, scopes, xAddrs, epr, instanceId, metadata_version=metadata_version)
        self._logger.info('publishing %r', service)
        self._localServices[epr] = service
        return service

    def clearService(self, epr):
        service = self._localServices[epr]
        self._sendBye(service)
        del self._localServices[epr]

    def getActiveAddresses(self):
        return self._networkingThread.getActiveAddresses()

    @staticmethod
    def generate_instance_id():
        return str(random.randint(1, 0xFFFFFFFF))


class WSDiscoveryBlacklist(WSDiscoveryBase):
    """ Binds to all IP addresses except the black listed ones. """

    def __init__(self, ignoredAdaptorIPAddresses=None, logger=None, multicast_port=None):
        """
        :param ignoredAdaptorIPAddresses: an optional list of (own) ip addresses that shall not be used for discovery.
                                          IP addresses are handled as regular expressions.
        """
        super(WSDiscoveryBlacklist, self).__init__(logger, multicast_port)
        tmp = [] if ignoredAdaptorIPAddresses is None else ignoredAdaptorIPAddresses
        self._ignoredAdaptorIPAddresses = [re.compile(x) for x in tmp]

    def _isAcceptedAddress(self, addr):
        """ check if any of the regular expressions matches the argument"""
        for x in self._ignoredAdaptorIPAddresses:
            if x.match(addr) is not None:
                return False
        return True


WSDiscovery = WSDiscoveryBlacklist  # deprecated name, for backward compatibility


class WSDiscoveryWhitelist(WSDiscoveryBase):
    """ Binds to all IP listed IP addresses. """

    def __init__(self, acceptedAdapterIPAddresses, logger=None, multicast_port=None):
        """
        :param acceptedAdaptorIPAddresses: an optional list of (own) ip addresses that shall not be used for discovery.
        """
        super(WSDiscoveryWhitelist, self).__init__(logger, multicast_port)
        tmp = [] if acceptedAdapterIPAddresses is None else acceptedAdapterIPAddresses
        self.acceptedAdapterIPAddresses = [re.compile(x) for x in tmp]

    def _isAcceptedAddress(self, addr):
        """ check if any of the regular expressions matches the argument"""
        for x in self.acceptedAdapterIPAddresses:
            if x.match(addr) is not None:
                return True
        return False


class WSDiscoverySingleAdapter(WSDiscoveryBase):
    """ Bind to a single adapter, identified by name.
    """

    def __init__(self, adapterName, logger=None, forceAdapterName=False, multicast_port=None):
        """
        :param adapterName: a string,  e.g. 'local area connection'.
                            parameter is only relevant if host has more than one adapter or forceName is True
                            If host has more than one adapter, the adapter with this friendly name is used, but if it does not exist, a RuntimeError is thrown.
        :param logger: use this logger. If none, 'sdc.discover' is used.
        :param forceAdapterName: if True, only this named adapter will be used.
                                 If False, and only one Adapter exists, the one existing adapter is used. (localhost is ignored in this case).
        """
        super().__init__(logger, multicast_port)

        all_adapters = getNetworkAdapterConfigs()
        # try to match name. if it matches, we are already ready.
        filteredAdapters = [a for a in all_adapters if a.friendly_name == adapterName]
        if len(filteredAdapters) == 1:
            self._myIPaddress = (filteredAdapters[0].ip,)  # a tuple
            return
        if forceAdapterName:
            raise RuntimeError(
                'No adapter "{}" found. Having {}'.format(adapterName, [a.friendly_name for a in all_adapters]))

        # see if there is only one physical adapter. if yes, use it
        adapters_not_localhost = [a for a in all_adapters if not a.ip.startswith('127.')]
        if len(adapters_not_localhost) == 1:
            self._myIPaddress = (adapters_not_localhost[0].ip,)  # a tuple
        else:
            raise RuntimeError('No adapter "{}" found. Cannot use default, having {}'.format(adapterName,
                                                                                             [a.friendly_name for a in
                                                                                              all_adapters]))

    def _isAcceptedAddress(self, addr):
        """ check if any of the regular expressions matches the argument"""
        return addr in self._myIPaddress


_FallbackMedicalDeviceTypesFilter = [QName(NS_DPWS, 'Device'),
                                     QName('http://standards.ieee.org/downloads/11073/11073-20702-2016',
                                           'MedicalDevice')]
