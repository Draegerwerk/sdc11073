import os
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
    EventingSchemaFile = os.path.join(schemaFolder, 'eventing.xsd')
    AddressingSchemaFile = os.path.join(schemaFolder, 'addressing.xsd')
    XMLSchemaFile = os.path.join(schemaFolder, 'xml.xsd')
    DPWSSchemaFile = os.path.join(schemaFolder, 'wsdd-dpws-1.1-schema-os.xsd')
    WSDiscoverySchemaFile = os.path.join(schemaFolder, 'wsdd-discovery-1.1-schema-os.xsd')
    WSDLSchemaFile = os.path.join(schemaFolder, 'wsdl.xsd')

    SchemaFilePaths = None

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

    @classmethod
    def get_schema_file_path(cls, url):
        return cls.SchemaFilePaths.schema_location_lookup.get(url)
