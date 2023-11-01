from . import xml_structure as cp
from .dpws_types import HostServiceType, HostedServiceType
from .dpws_types import ThisDeviceType, ThisModelType, DeviceMetadataDialectURI, DeviceRelationshipTypeURI
from .msg_types import MessageType
from .pm_types import PropertyBasedPMType
from sdc11073.namespaces import default_ns_helper

########## Meta Data Exchange #########
wsx_tag = default_ns_helper.WSX.tag  # shortcut
dpws_tag = default_ns_helper.DPWS.tag  # shortcut
wsa_tag = default_ns_helper.WSA.tag  # shortcut


class GetMetadata(MessageType):
    NODETYPE = wsx_tag('GetMetadata')
    action = 'http://schemas.xmlsoap.org/ws/2004/09/mex/GetMetadata/Request'
    Dialect = cp.AnyUriTextElement(wsx_tag('Dialect'), is_optional=True)
    Identifier = cp.AnyUriTextElement(wsx_tag('Identifier'), is_optional=True)
    _props = ('Dialect', 'Identifier')


class ThisModelMetadataSection(PropertyBasedPMType):
    MetadataReference = cp.SubElementProperty(dpws_tag('ThisModel'),
                                              value_class=ThisModelType)
    Location = cp.AnyUriTextElement(wsx_tag('Location'), is_optional=True)
    Dialect = cp.AnyURIAttributeProperty('Dialect',
                                         default_py_value=DeviceMetadataDialectURI.THIS_MODEL,
                                         is_optional=False)
    Identifier = cp.AnyURIAttributeProperty('Identifier')
    _props = ('MetadataReference', 'Location', 'Dialect', 'Identifier')


class ThisDeviceMetadataSection(PropertyBasedPMType):
    MetadataReference = cp.SubElementProperty(dpws_tag('ThisDevice'),
                                              value_class=ThisDeviceType)
    Location = cp.AnyUriTextElement(wsx_tag('Location'), is_optional=True)
    Dialect = cp.AnyURIAttributeProperty('Dialect',
                                         default_py_value=DeviceMetadataDialectURI.THIS_DEVICE,
                                         is_optional=False)
    Identifier = cp.AnyURIAttributeProperty('Identifier')
    _props = ('MetadataReference', 'Location', 'Dialect', 'Identifier')


class MetaDataRelationship(PropertyBasedPMType):
    Type = cp.AnyURIAttributeProperty(attribute_name='Type', default_py_value=DeviceRelationshipTypeURI.HOST)
    Host = cp.SubElementProperty(dpws_tag('Host'), value_class=HostServiceType)
    Hosted = cp.SubElementListProperty(dpws_tag('Hosted'), value_class=HostedServiceType)
    _props = ('Type', 'Host', 'Hosted')


class RelationshipMetadataSection(PropertyBasedPMType):
    MetadataReference = cp.SubElementProperty(dpws_tag('Relationship'),
                                              value_class=MetaDataRelationship,
                                              default_py_value=MetaDataRelationship())
    Location = cp.AnyUriTextElement(wsx_tag('Location'), is_optional=True)
    Dialect = cp.AnyURIAttributeProperty('Dialect',
                                         default_py_value=DeviceMetadataDialectURI.RELATIONSHIP,
                                         is_optional=False)
    Identifier = cp.AnyURIAttributeProperty('Identifier')
    _props = ('MetadataReference', 'Location', 'Dialect', 'Identifier')


class LocationMetadataSection(PropertyBasedPMType):
    Location = cp.AnyUriTextElement(wsx_tag('Location'), is_optional=True)
    Dialect = cp.AnyURIAttributeProperty('Dialect',
                                         default_py_value=default_ns_helper.WSDL.namespace,
                                         is_optional=False)
    Identifier = cp.AnyURIAttributeProperty('Identifier')
    _props = ('Location', 'Dialect', 'Identifier')


dialect_lookup = {DeviceMetadataDialectURI.THIS_MODEL: (ThisModelMetadataSection,
                                                        'this_model',
                                                        'MetadataReference'),
                  DeviceMetadataDialectURI.THIS_DEVICE: (ThisDeviceMetadataSection,
                                                         'this_device',
                                                         'MetadataReference'),
                  DeviceMetadataDialectURI.RELATIONSHIP: (RelationshipMetadataSection,
                                                          'relationship',
                                                          'MetadataReference'),
                  default_ns_helper.WSDL.namespace: (LocationMetadataSection,
                                                     'wsdl_location',
                                                     'Location')
                  }


class Metadata(MessageType):
    NODETYPE = wsx_tag('Metadata')
    action = f'{default_ns_helper.WXF.namespace}/GetResponse'
    MetadataSection = cp.SubElementListProperty(wsx_tag('MetadataSection'),
                                                value_class=PropertyBasedPMType)
    _props = ('MetadataSection',)

    def __init__(self):
        super().__init__()
        self.this_model = None
        self.this_device = None
        self.wsdl_location = None
        self.relationship = None

    @classmethod
    def from_node(cls, node):
        """ default from_node Constructor that provides no arguments for class __init__"""
        meta_data = cls()
        for section_node in node[0]:
            dialect = section_node.attrib.get('Dialect')
            section_cls, member_name, section_member_name = dialect_lookup.get(dialect)
            if section_cls is not None:
                section = section_cls.from_node(section_node)
                meta_data.MetadataSection.append(section)
                setattr(meta_data, member_name, getattr(section, section_member_name))
            else:
                print(f'unknown dialect {dialect}')
        return meta_data
