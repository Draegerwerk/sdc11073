import os

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
    def normalize_xml_text(cls, xml_text: bytes) -> bytes:
        """ replace BICEPS namespaces with internal namespaces"""
        for namespace, internal_ns in ((cls.MessageModelNamespace, Prefixes.MSG.namespace),
                                       (cls.ParticipantModelNamespace, Prefixes.PM.namespace),
                                       (cls.ExtensionPointNamespace, Prefixes.EXT.namespace),
                                       (cls.MDPWSNameSpace, Prefixes.MDPWS.namespace)):
            xml_text = xml_text.replace(f'"{namespace}"'.encode('utf-8'),
                                        f'"{internal_ns}"'.encode('utf-8'))
        return xml_text

    @classmethod
    def denormalize_xml_text(cls, xml_text: bytes) -> bytes:
        """ replace internal namespaces with BICEPS namespaces"""
        for namespace, internal_ns in ((cls.MessageModelNamespace.encode('utf-8'), b'__BICEPS_MessageModel__'),
                                       (cls.ParticipantModelNamespace.encode('utf-8'), b'__BICEPS_ParticipantModel__'),
                                       (cls.ExtensionPointNamespace.encode('utf-8'), b'__ExtensionPoint__'),
                                       (cls.MDPWSNameSpace.encode('utf-8'), b'__MDPWS__')):
            xml_text = xml_text.replace(internal_ns, namespace)
        return xml_text

    @classmethod
    def get_schema_file_path(cls, url):
        return cls.SchemaFilePaths.schema_location_lookup.get(url)
