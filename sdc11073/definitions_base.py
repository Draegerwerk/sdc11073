import os
import traceback
import urllib

from lxml import etree as etree_

from . import loghelper
from .namespaces import Prefixes
from .namespaces import dpwsTag

schemaFolder = os.path.join(os.path.dirname(__file__), 'xsd')


class ProtocolsRegistry(type):
    """
    base class that has the only purpose to register classes that use this as meta class
    """
    protocols = []

    def __new__(cls, name, *arg, **kwarg):
        new_cls = super().__new__(cls, name, *arg, **kwarg)
        if name != 'BaseDefinitions':  # ignore the base class itself
            cls.protocols.append(new_cls)
        return new_cls


# definitions that group all relevant dependencies for BICEPS versions
class BaseDefinitions(metaclass=ProtocolsRegistry):
    """ Central definitions for SDC
    It defines namespaces and handlers for the protocol.
    Derive from this class in order to define different protocol handling."""
    DpwsDeviceType = dpwsTag('Device')
    SchemaFilePaths = None
    # set the following namespaces in derived classes:
    MedicalDeviceTypeNamespace = None
    BICEPSNamespace = None
    MessageModelNamespace = None
    ParticipantModelNamespace = None
    ExtensionPointNamespace = None
    MedicalDeviceType = None
    ActionsNamespace = None
    DefaultSdcDeviceComponents = None
    DefaultSdcClientComponents = None
    MDPWSNameSpace = None

    @classmethod
    def ns_matches(cls, namespace):
        """ This method checks if this definition set is the correct one for a given namespace"""
        return namespace in (cls.MedicalDeviceTypeNamespace, cls.BICEPSNamespace, cls.MessageModelNamespace,
                             cls.ParticipantModelNamespace, cls.ExtensionPointNamespace, cls.MedicalDeviceType)

    @classmethod
    def normalize_xml_text(cls, xml_text):
        """ replace BICEPS namespaces with internal namespaces"""
        for namespace, internal_ns in ((cls.MessageModelNamespace, Prefixes.MSG.namespace),
                                       (cls.ParticipantModelNamespace, Prefixes.PM.namespace),
                                       (cls.ExtensionPointNamespace, Prefixes.EXT.namespace),
                                       (cls.MDPWSNameSpace, Prefixes.MDPWS.namespace)):
            xml_text = xml_text.replace('"{}"'.format(namespace).encode('utf-8'),
                                        '"{}"'.format(internal_ns).encode('utf-8'))
        return xml_text

    @classmethod
    def denormalize_xml_text(cls, xml_text):
        """ replace internal namespaces with BICEPS namespaces"""
        for namespace, internal_ns in ((cls.MessageModelNamespace.encode('utf-8'), b'__BICEPS_MessageModel__'),
                                       (cls.ParticipantModelNamespace.encode('utf-8'), b'__BICEPS_ParticipantModel__'),
                                       (cls.ExtensionPointNamespace.encode('utf-8'), b'__ExtensionPoint__'),
                                       (cls.MDPWSNameSpace.encode('utf-8'), b'__MDPWS__')):
            xml_text = xml_text.replace(internal_ns, namespace)
        return xml_text

def _is_biceps_schema_file(filename):
    return filename.endswith('ExtensionPoint.xsd') or \
           filename.endswith('BICEPS_ParticipantModel.xsd') or \
           filename.endswith('BICEPS_MessageModel.xsd')


class SchemaResolverBase(etree_.Resolver):
    lookup = {'http://schemas.xmlsoap.org/ws/2004/08/addressing': 'AddressingSchemaFile',
              'http://www.w3.org/2005/08/addressing/ws-addr.xsd': 'WsAddrSchemaFile',
              'http://www.w3.org/2005/08/addressing': 'WsAddrSchemaFile',
              'http://www.w3.org/2006/03/addressing/ws-addr.xsd': 'WsAddrSchemaFile',
              'http://schemas.xmlsoap.org/ws/2004/08/eventing/eventing.xsd': 'EventingSchemaFile',
              Prefixes.DPWS.namespace: 'DPWSSchemaFile',
              'http://schemas.xmlsoap.org/ws/2004/09/mex/MetadataExchange.xsd': 'MetaDataExchangeSchemaFile',
              'http://www.w3.org/2001/xml.xsd': 'XMLSchemaFile', }
    lookup_ext = {}  # to be overridden by derived classes

    def __init__(self, base_definitions, log_prefix=None):
        super().__init__()
        self._base_definitions = base_definitions
        self._logger = loghelper.get_logger_adapter('sdc.schema_resolver', log_prefix)

    def resolve(self, url, id, context):  # pylint: disable=unused-argument, redefined-builtin, invalid-name
        # first check if there is a lookup defined
        ref = self.lookup.get(url)
        if ref is None:
            ref = self.lookup_ext.get(url)
        if ref is not None:
            try:
                filename = getattr(self._base_definitions.SchemaFilePaths, ref)
            except AttributeError:
                self._logger.warn('could not resolve ref={}', ref)
                return None
            self._logger.debug('could resolve url {} via lookup to {}', url, filename)
            if not os.path.exists(filename):
                self._logger.warn('could resolve url {} via lookup, but path {} does not exist', url, filename)
                return None
            with open(filename, 'rb') as my_file:
                xml_text = my_file.read()
            if _is_biceps_schema_file(filename):
                xml_text = self._base_definitions.normalize_xml_text(xml_text)
            return self.resolve_string(xml_text, context, base_url=filename)

        # no lookup, parse url
        parsed = urllib.parse.urlparse(url)
        if parsed.scheme == 'file':
            path = parsed.path  # get the path part
        else:  # the url is a path
            path = url
        if path.startswith('/') and path[2] == ':':  # invalid construct like /C:/Temp
            path = path[1:]
        if not os.path.exists(path):
            self._logger.warn('could not resolve url {}, path {} does not exist', url, path)
            return None
        self._logger.debug('could resolve url {}: path = {}', url, path)
        with open(path, 'rb') as the_file:
            xml_text = the_file.read()
        if _is_biceps_schema_file(path):
            xml_text = self._base_definitions.normalize_xml_text(xml_text)
        return self.resolve_string(xml_text, context, base_url=path)
