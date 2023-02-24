from __future__ import annotations

from enum import Enum
from typing import List, Union, Optional, TYPE_CHECKING

from lxml.etree import QName

if TYPE_CHECKING:
    from .addressing import EndpointReferenceType


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


class HostServiceType:
    __slots__ = ('endpoint_reference', 'types')

    def __init__(self,
                 endpoint_reference: list,
                 types_list: List[QName]):
        """
        :param endpoint_references: list of etree_ nodes
        :param types_list: a list of etree.QName instances
        """
        self.endpoint_reference = endpoint_reference
        self.types = types_list

    def __str__(self):
        return f'HostServiceType: endpointReference={self.endpoint_reference}, types="{self.types}"'


class HostedServiceType:
    __slots__ = ('endpoint_references', 'types', 'service_id')

    def __init__(self,
                 endpoint_references_list: List[EndpointReferenceType],
                 types_list: List[QName],
                 service_id: str):
        self.endpoint_references: List[EndpointReferenceType] = endpoint_references_list
        self.types: List[QName] = types_list
        self.service_id: str = service_id

    def __str__(self):
        return f'HostedServiceType: endpointReference={self.endpoint_references}, types="{self.types}" ' \
               f'service_id="{self.service_id}"'


class ThisDeviceType:
    __slots__ = ('friendly_name', 'firmware_version', 'serial_number')

    def __init__(self, friendly_name: Union[str, LocalizedStringTypeDict],
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
        if isinstance(friendly_name, str):
            self.friendly_name = LocalizedStringTypeDict({None: friendly_name})  # localized texts, default name
        else:
            assert (isinstance(friendly_name, LocalizedStringTypeDict))
            self.friendly_name = friendly_name
        self.firmware_version = firmware_version
        self.serial_number = serial_number

    def __str__(self):
        return f'ThisDeviceType: friendly_name={self.friendly_name}, ' \
               f'firmware_version="{self.firmware_version}", ' \
               f'serial_number="{self.serial_number}"'

    def __eq__(self, other):
        try:
            for slot in self.__slots__:
                if getattr(self, slot) != getattr(other, slot):
                    return False
            return True
        except AttributeError:
            return False


class ThisModelType:
    __slots__ = ('manufacturer', 'manufacturer_url', 'model_name', 'model_number', 'model_url', 'presentation_url')

    def __init__(self,
                 manufacturer: Union[str, LocalizedStringTypeDict],
                 manufacturer_url: str,
                 model_name: Union[str, LocalizedStringTypeDict],
                 model_number: str,
                 model_url: str,
                 presentation_url: str):
        """
        This class represents "ThisModelType" in dpws schema.
        :param manufacturer:
        :param manufacturer_url:
        :param model_name:
        :param model_number:
        :param model_url:
        :param presentation_url:
        """
        if isinstance(manufacturer, str):
            self.manufacturer = LocalizedStringTypeDict({None: manufacturer})
        else:
            assert (isinstance(manufacturer, LocalizedStringTypeDict))
            self.manufacturer = manufacturer
        self.manufacturer_url = manufacturer_url
        if isinstance(model_name, str):
            self.model_name = LocalizedStringTypeDict({None: model_name})
        else:
            assert (isinstance(model_name, LocalizedStringTypeDict))
            self.model_name = model_name
        self.model_number = model_number
        self.model_url = model_url
        self.presentation_url = presentation_url

    def __str__(self):
        return f'ThisModelType: manufacturer={self.manufacturer}, model_name="{self.model_name}", ' \
               f'model_number="{self.model_number}"'

    def __eq__(self, other):
        try:
            for slot in self.__slots__:
                if getattr(self, slot) != getattr(other, slot):
                    return False
            return True
        except AttributeError:
            return False

# class LocalizedStringType(cp.NodeStringProperty):
#     lang = cp.StringAttributeProperty(default_ns_helper.xmlTag('lang'))
#     _props = ['lang']
#
#     def __init__(self, lang, text):
#         super().__init__()
#         self.lang = lang
#         self.text = text
#
#
# class ThisModelType(PropertyBasedPMType):
#     Manufacturer = cp.SubElementListProperty(default_ns_helper.dpwsTag('Manufacturer'),
#                                              value_class=LocalizedStringType)
#     ManufacturerUrl = cp.NodeStringProperty(default_ns_helper.dpwsTag('ManufacturerUrl'))
#     ModelName = cp.SubElementListProperty(default_ns_helper.dpwsTag('ModelName'),
#                                              value_class=LocalizedStringType)
#     ModelNumber = cp.NodeStringProperty(default_ns_helper.dpwsTag('ModelNumber'), is_optional=True)
#     ModelUrl = cp.NodeStringProperty(default_ns_helper.dpwsTag('ModelUrl'), is_optional=True)
#     PresentationUrl = cp.NodeStringProperty(default_ns_helper.dpwsTag('PresentationUrl'), is_optional=True)
#
#
#     def __init__(self,
#                  manufacturer: Union[str, LocalizedStringTypeDict],
#                  manufacturer_url: str,
#                  model_name: Union[str, LocalizedStringTypeDict],
#                  model_number: str,
#                  model_url: str,
#                  presentation_url: str):
#
#         if isinstance(manufacturer, str):
#             self.Manufacturer.append(LocalizedStringType(None, manufacturer))
#         else:
#             assert(isinstance(manufacturer, LocalizedStringTypeDict))
#             for lang, value in manufacturer.items():
#                 self.Manufacturer.append(LocalizedStringType(lang, value))
#         self.ManufacturerUrl = manufacturer_url
#         if isinstance(model_name, str):
#             self.ModelName.append(LocalizedStringType(None, model_name))
#         else:
#             assert(isinstance(model_name, LocalizedStringTypeDict))
#             for lang, value in model_name.items():
#                 self.ModelName.append(LocalizedStringType(lang, value))
#         self.ModelNumber = model_number
#         self.ModelUrl = model_url
#         self.PresentationUrl = presentation_url
#
