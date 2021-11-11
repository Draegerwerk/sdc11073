from __future__ import annotations

import os
import urllib
from dataclasses import dataclass
from typing import Type, Callable, List, Any, TYPE_CHECKING  # ForwardRef

from lxml import etree as etree_
from lxml.etree import QName

from . import loghelper
from .namespaces import Prefixes
from .namespaces import dpwsTag
from .wsdiscovery import Scope

# pylint: disable=cyclic-import
if TYPE_CHECKING:
    from .pysoap.msgfactory import AbstractMessageFactory
    from .pysoap.msgreader import AbstractMessageReader
    from .sdcdevice.sdc_handlers import HostedServices
    from .sdcdevice.sco import AbstractScoOperationsRegistry
    from .sdcdevice.subscriptionmgr import AbstractSubscriptionsManager
    from .mdib.devicemdib import DeviceMdibContainer
    from .httprequesthandler import RequestData
# pylint: enable=cyclic-import

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


# Dependency injection: This class defines which component implementations the sdc client will use.
@dataclass()
class SdcClientComponents:
    msg_factory_class: type = None
    msg_reader_class: type = None
    notifications_receiver_class: type = None
    notifications_handler_class: type = None
    notifications_dispatcher_class: type = None
    subscription_manager_class: type = None
    operations_manager_class: type = None
    service_handlers: dict = None

    def merge(self, other):
        def _merge(attrname):
            other_value = getattr(other, attrname)
            if other_value:
                setattr(self, attrname, other_value)

        _merge('msg_factory_class')
        _merge('msg_reader_class')
        _merge('notifications_receiver_class')
        _merge('notifications_handler_class')
        _merge('subscription_manager_class')
        _merge('operations_manager_class')
        if other.service_handlers:
            for key, value in other.service_handlers.items():
                self.service_handlers[key] = value


# Dependency injection: This class defines which component implementations the sdc device will use.
@dataclass()
class SdcDeviceComponents:
    msg_factory_class: Type[AbstractMessageFactory] = None
    msg_reader_class: Type[AbstractMessageReader] = None
    xml_reader_class: Type[AbstractMessageReader] = None  # needed to read xml based mdib files
    services_factory: Callable[[Any, dict, Any], HostedServices] = None
    operation_cls_getter: Callable[[QName], type] = None
    sco_operations_registry_class: Type[AbstractScoOperationsRegistry] = None
    subscriptions_manager_class: Type[AbstractSubscriptionsManager] = None
    role_provider_class: type = None
    scopes_factory: Callable[[DeviceMdibContainer], List[Scope]] = None
    msg_dispatch_method: Callable[[RequestData], str] = None
    service_handlers: dict = None

    def merge(self, other):
        def _merge(attr_name):
            other_value = getattr(other, attr_name)
            if other_value:
                setattr(self, attr_name, other_value)

        _merge('msg_factory_class')
        _merge('msg_reader_class')
        _merge('services_factory')
        _merge('operation_cls_getter')
        _merge('sco_operations_registry_class')
        _merge('subscriptions_manager_class')
        _merge('role_provider_class')
        _merge('scopes_factory')
        if other.service_handlers:
            for key, value in other.service_handlers.items():
                self.service_handlers[key] = value


# definitions that group all relevant dependencies for BICEPS versions
class BaseDefinitions(metaclass=ProtocolsRegistry):
    """ Base class for central definitions used by SDC.
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

    get_descriptor_container_class = None
    get_state_container_class = None

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
            xml_text = xml_text.replace(f'"{namespace}"'.encode('utf-8'),
                                        f'"{internal_ns}"'.encode('utf-8'))
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

    @classmethod
    def get_schema_file_path(cls, url):
        return cls.SchemaFilePaths.namespace_schema_file_lookup.get(url)


def _needs_normalize(filename):
    return filename.endswith('ExtensionPoint.xsd') or \
           filename.endswith('BICEPS_ParticipantModel.xsd') or \
           filename.endswith('BICEPS_MessageModel.xsd')


class SchemaValidators:
    def __init__(self, definition_cls):
        """
        Contains instances of XMLSchema validators
        :param definition_cls: a class derived from BaseDefinitions, it contains paths to xml schema files
        """
        self.parser = etree_.ETCompatXMLParser(resolve_entities=False)
        self._definitions = definition_cls
        self.parser.resolvers.add(SchemaResolver(definition_cls))
        schema_paths = self._definitions.SchemaFilePaths

        self.participant_schema = self._mk_schema(schema_paths.ParticipantModelSchemaFile, normalize=True)
        self.message_schema = self._mk_schema(schema_paths.MessageModelSchemaFile, normalize=True)
        self.mex_schema = self._mk_schema(schema_paths.MetaDataExchangeSchemaFile)
        self.eventing_schema = self._mk_schema(schema_paths.EventingSchemaFile)
        self.soap12_schema = self._mk_schema(schema_paths.SoapEnvelopeSchemaFile)
        self.dpws_schema = self._mk_schema(schema_paths.DPWSSchemaFile)
        self.wsdl_schema = self._mk_schema(schema_paths.WSDLSchemaFile)

    def __str__(self):
        return f'{self.__class__.__name__} {self._definitions.__name__}'

    def _mk_schema(self, path, normalize=False):
        with open(path, 'rb') as _file:
            xml_text = _file.read()
        if normalize:
            xml_text = self._definitions.normalize_xml_text(xml_text)
        elem_tree = etree_.fromstring(xml_text, parser=self.parser, base_url=path)
        return etree_.XMLSchema(etree=elem_tree)


class SchemaResolver(etree_.Resolver):

    def __init__(self, base_definitions, log_prefix=None):
        super().__init__()
        self._base_definitions = base_definitions
        self._logger = loghelper.get_logger_adapter('sdc.schema_resolver', log_prefix)

    def resolve(self, url, id, context):  # pylint: disable=unused-argument, redefined-builtin, invalid-name
        # first check if there is a lookup defined
        path = self._base_definitions.get_schema_file_path(url)
        if path:
            self._logger.debug('could resolve url {} via lookup to {}', url, path)
        else:
            # no lookup, parse url
            parsed = urllib.parse.urlparse(url)
            if parsed.scheme == 'file':
                path = parsed.path  # get the path part
            else:  # the url is a path
                path = url
            if path.startswith('/') and path[2] == ':':  # invalid construct like /C:/Temp
                path = path[1:]

        if not os.path.exists(path):
            self._logger.error('no schema file for url "{}": resolved to "{}", but file does not exist', url, path)
            return None
        with open(path, 'rb') as my_file:
            xml_text = my_file.read()
        if _needs_normalize(path):
            xml_text = self._base_definitions.normalize_xml_text(xml_text)
        return self.resolve_string(xml_text, context, base_url=path)
