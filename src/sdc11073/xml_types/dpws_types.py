from __future__ import annotations

from enum import Enum
from typing import Union, Optional

from .addressing_types import EndpointReferenceType
from . import xml_structure as cp
from sdc11073.namespaces import default_ns_helper
from .basetypes import XMLTypeBase, ElementWithText


class DeviceRelationshipTypeURI(str, Enum):
    HOST = "http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/host"


class DeviceMetadataDialectURI(str, Enum):
    THIS_MODEL = "http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/ThisModel"
    THIS_DEVICE = "http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/ThisDevice"
    RELATIONSHIP = "http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/Relationship"


class DeviceEventingFilterDialectURI(str, Enum):
    ACTION = "http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/Action"


class Relationship:
    __slots__ = ('host', 'hosted')

    def __init__(self):
        self.host = None
        self.hosted = {}


class LocalizedStringTypeDict(dict):
    """This class represents LocalizedStringType elements. It is a dictionary of lang:string entries.
     If lang is None, the_string is the default string."""

    def add_localized_string(self, the_string: str, lang: Optional[str] = None) -> None:
        """
        Method for better readability of code
        :param the_string:
        :param lang:
        :return: None
        """
        self[lang] = the_string


class HostServiceType(XMLTypeBase):
    EndpointReference = cp.SubElementProperty(default_ns_helper.WSA.tag('EndpointReference'),
                                              value_class=EndpointReferenceType)
    Types = cp.NodeTextQNameListProperty(default_ns_helper.DPWS.tag('Types'))
    _props = ('EndpointReference', 'Types')


class HostedServiceType(XMLTypeBase):
    EndpointReference = cp.SubElementListProperty(default_ns_helper.WSA.tag('EndpointReference'),
                                                  value_class=EndpointReferenceType)
    Types = cp.NodeTextQNameListProperty(default_ns_helper.DPWS.tag('Types'))
    ServiceId = cp.AnyUriTextElement(default_ns_helper.DPWS.tag('ServiceId'))
    _props = ('EndpointReference', 'Types', 'ServiceId')


class LocalizedStringType(ElementWithText):
    lang = cp.StringAttributeProperty(default_ns_helper.XML.tag('lang'))
    _props = ('lang',)

    @classmethod
    def init(cls, text: str, lang: Optional[str] = None):
        """
        This class represents the LocalizedStringType in DPWS.
        :param text: the text
        :param lang: if given, the actual language
        :return:
        """
        instance = cls()
        instance.lang = lang
        instance.text = text
        return instance


class ThisDeviceType(XMLTypeBase):
    """
    This class represents "ThisDeviceType" in dpws schema.
    """
    FriendlyName = cp.SubElementListProperty(default_ns_helper.DPWS.tag('FriendlyName'),
                                             value_class=LocalizedStringType)
    FirmwareVersion = cp.NodeStringProperty(default_ns_helper.DPWS.tag('FirmwareVersion'), is_optional=True)
    SerialNumber = cp.NodeStringProperty(default_ns_helper.DPWS.tag('SerialNumber'), is_optional=True)
    _props = ('FriendlyName', 'FirmwareVersion', 'SerialNumber')

    def __init__(self, friendly_name: Union[str, LocalizedStringTypeDict, None] = None,
                 firmware_version: Optional[str] = None,
                 serial_number: Optional[str] = None):
        """
        This class represents "ThisDeviceType" in dpws schema.
        :param friendly_name: If argument is a string, it is considered to be the default name.
                              If argument is a dictionary, it is expected to be key=language, value=name.
                              None as key marks the default name.
        :param firmware_version: any string
        :param serial_number: any string
        """
        super().__init__()
        if isinstance(friendly_name, str):
            self.FriendlyName.append(LocalizedStringType.init(friendly_name))
        elif isinstance(friendly_name, LocalizedStringTypeDict):
            for lang, text in friendly_name.items():
                self.FriendlyName.append(LocalizedStringType.init(text, lang))
        self.FirmwareVersion = firmware_version
        self.SerialNumber = serial_number


class ThisModelType(XMLTypeBase):
    Manufacturer = cp.SubElementListProperty(default_ns_helper.DPWS.tag('Manufacturer'),
                                             value_class=LocalizedStringType)
    ManufacturerUrl = cp.NodeStringProperty(default_ns_helper.DPWS.tag('ManufacturerUrl'))
    ModelName = cp.SubElementListProperty(default_ns_helper.DPWS.tag('ModelName'),
                                          value_class=LocalizedStringType)
    ModelNumber = cp.NodeStringProperty(default_ns_helper.DPWS.tag('ModelNumber'), is_optional=True)
    ModelUrl = cp.NodeStringProperty(default_ns_helper.DPWS.tag('ModelUrl'), is_optional=True)
    PresentationUrl = cp.NodeStringProperty(default_ns_helper.DPWS.tag('PresentationUrl'), is_optional=True)
    _props = ('Manufacturer', 'ManufacturerUrl', 'ModelName', 'ModelNumber', 'ModelUrl', 'PresentationUrl')

    def __init__(self,
                 manufacturer: Union[str, LocalizedStringTypeDict, None] = None,
                 manufacturer_url: Optional[str] = None,
                 model_name: Union[str, LocalizedStringTypeDict, None] = None,
                 model_number: Optional[str] = None,
                 model_url: Optional[str] = None,
                 presentation_url: Optional[str] = None):
        super().__init__()
        if isinstance(manufacturer, str):
            self.Manufacturer.append(LocalizedStringType.init(manufacturer))
        elif isinstance(manufacturer, LocalizedStringTypeDict):
            for lang, text in manufacturer.items():
                self.Manufacturer.append(LocalizedStringType.init(text, lang))
        self.ManufacturerUrl = manufacturer_url
        if isinstance(model_name, str):
            self.ModelName.append(LocalizedStringType.init(model_name))
        elif isinstance(model_name, LocalizedStringTypeDict):
            for lang, text in model_name.items():
                self.ModelName.append(LocalizedStringType.init(text, lang))
        self.ModelNumber = model_number
        self.ModelUrl = model_url
        self.PresentationUrl = presentation_url

