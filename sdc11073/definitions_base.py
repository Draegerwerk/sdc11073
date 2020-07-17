import os
import urllib
import traceback
from lxml import etree as etree_
from . import loghelper
from .namespaces import dpwsTag
from .namespaces import Prefix_Namespace as Prefix
schemaFolder = os.path.join(os.path.dirname(__file__), 'xsd')


class ProtocolsRegistry(type):
    '''
    base class that has the only purpose to register classes that use this as meta class
    '''
    protocols = []

    def __new__(cls, name, *arg, **kwarg):
        new_cls = super().__new__(cls, name, *arg, **kwarg)
        if name != 'BaseDefinitions': # ignore the base class itself
            cls.protocols.append(new_cls)
        return new_cls


# definitions that group all relevant dependencies for BICEPS versions
class BaseDefinitions(metaclass=ProtocolsRegistry):
    ''' Central definitions for SDC
    It defines namespaces and handlers for the protocol.
    Derive from this class in order to define different protocol handling.'''
    DpwsDeviceType = dpwsTag('Device')
    MetaDataExchangeSchemaFile = os.path.join(schemaFolder, 'MetadataExchange.xsd')
    EventingSchemaFile = os.path.join(schemaFolder, 'eventing.xsd')
    SoapEnvelopeSchemaFile = os.path.join(schemaFolder, 'soap-envelope.xsd')
    WsAddrSchemaFile = os.path.join(schemaFolder, 'ws-addr.xsd')
    AddressingSchemaFile = os.path.join(schemaFolder, 'addressing.xsd')
    XMLSchemaFile = os.path.join(schemaFolder, 'xml.xsd')
    DPWSSchemaFile = os.path.join(schemaFolder, 'wsdd-dpws-1.1-schema-os.xsd')
    # set the following namespaces in derived classes:
    MedicalDeviceTypeNamespace = None
    BICEPSNamespace = None
    MessageModelNamespace = None
    ParticipantModelNamespace = None
    ExtensionPointNamespace = None
    MedicalDeviceType = None
    ActionsNamespace = None

    @classmethod
    def ns_matches(cls, ns):
        ''' This method checks if this definition set is the correct one for a given namespace'''
        return ns in (cls.MedicalDeviceTypeNamespace, cls.BICEPSNamespace, cls.MessageModelNamespace, cls.ParticipantModelNamespace, cls.ExtensionPointNamespace, cls.MedicalDeviceType)

    @classmethod
    def normalizeXMLText(cls, xml_text):
        ''' replace BICEPS namespaces with internal namespaces'''
        for ns, internal_ns in ((cls.MessageModelNamespace, Prefix.MSG.namespace), #'__BICEPS_MessageModel__'),
                                (cls.ParticipantModelNamespace, Prefix.PM.namespace), #'__BICEPS_ParticipantModel__'),
                                (cls.ExtensionPointNamespace, Prefix.EXT.namespace), #'__ExtensionPoint__'),
                                (cls.MDPWSNameSpace, Prefix.MDPWS.namespace)): #'__MDPWS__')):
            xml_text = xml_text.replace('"{}"'.format(ns).encode('utf-8'), '"{}"'.format(internal_ns).encode('utf-8'))
        return xml_text

    @classmethod
    def denormalizeXMLText(cls, xml_text):
        ''' replace internal namespaces with BICEPS namespaces'''
        for ns, internal_ns in ((cls.MessageModelNamespace.encode('utf-8'), b'__BICEPS_MessageModel__'),
                                (cls.ParticipantModelNamespace.encode('utf-8'), b'__BICEPS_ParticipantModel__'),
                                (cls.ExtensionPointNamespace.encode('utf-8'), b'__ExtensionPoint__'),
                                (cls.MDPWSNameSpace.encode('utf-8'), b'__MDPWS__')):
            xml_text = xml_text.replace(internal_ns, ns)
        return xml_text


class SchemaResolverBase(etree_.Resolver):
    lookup = {'http://schemas.xmlsoap.org/ws/2004/08/addressing': 'AddressingSchemaFile',
              'http://www.w3.org/2005/08/addressing/ws-addr.xsd': 'WsAddrSchemaFile',
              'http://www.w3.org/2005/08/addressing': 'WsAddrSchemaFile',
              'http://www.w3.org/2006/03/addressing/ws-addr.xsd': 'WsAddrSchemaFile',
              'http://schemas.xmlsoap.org/ws/2004/08/eventing/eventing.xsd': 'EventingSchemaFile',
              Prefix.DPWS.namespace: 'DPWSSchemaFile',
              'http://schemas.xmlsoap.org/ws/2004/09/mex/MetadataExchange.xsd': 'MetaDataExchangeSchemaFile',
              'http://www.w3.org/2001/xml.xsd': 'XMLSchemaFile',}
    lookup_ext = {} # to be overridden by derived classes
    def __init__(self, baseDefinitions, log_prefix=None):
        super(SchemaResolverBase, self).__init__()
        self._baseDefinitions = baseDefinitions
        self._logger = loghelper.getLoggerAdapter('sdc.schema_resolver', log_prefix)

    def _isBicepsSchemaFile(self, filename):
        return filename.endswith('ExtensionPoint.xsd') or filename.endswith('BICEPS_ParticipantModel.xsd') or filename.endswith('BICEPS_MessageModel.xsd')

    def resolve(self, url, id, context):  # pylint: disable=unused-argument, redefined-builtin
        try:
            # first check if there is a lookup defined
            ref = self.lookup.get(url)
            if ref is None:
                ref = self.lookup_ext.get(url)
            if ref is not None:
                filename = getattr(self._baseDefinitions, ref)
                self._logger.debug('could resolve url {} via lookup to {}', url, filename)
                if not os.path.exists(filename):
                    self._logger.warn('could resolve url {} via lookup, but path {} does not exist', url, filename)
                    return
                with open(filename, 'rb') as f:
                    xml_text = f.read()
                if self._isBicepsSchemaFile(filename):
                    xml_text = self._baseDefinitions.normalizeXMLText(xml_text)
                return self.resolve_string(xml_text, context, base_url=filename)

            # no lookup, parse url
            parsed = urllib.parse.urlparse(url)
            if parsed.scheme == 'file':
                path = parsed.path # get the path part
            else: # the url is a path
                path = url
            if path.startswith('/') and path[2] == ':':  # invalid construct like /C:/Temp
                path = path[1:]
            if not os.path.exists(path):
                self._logger.warn('could not resolve url {}, path {} does not exist', url, path)
                return
            else:
                self._logger.debug('could resolve url {}: path = {}', url, path)
                with open(path, 'rb') as f:
                    xml_text = f.read()
                if self._isBicepsSchemaFile(path):
                    xml_text = self._baseDefinitions.normalizeXMLText(xml_text)
                return self.resolve_string(xml_text, context, base_url=path)
        except:
            self._logger.error(traceback.format_exc())

