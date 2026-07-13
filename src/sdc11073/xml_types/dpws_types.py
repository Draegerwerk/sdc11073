"""DPWS (Devices Profile for Web Services) XML types."""

from __future__ import annotations

from enum import Enum

from sdc11073.namespaces import default_ns_helper
from sdc11073.xml_types import xml_structure as cp
from sdc11073.xml_types.addressing_types import EndpointReferenceType
from sdc11073.xml_types.basetypes import ElementWithText, XMLTypeBase


class DeviceRelationshipTypeURI(str, Enum):
    """URIs identifying DPWS device relationship types."""

    HOST = 'http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/host'


class DeviceMetadataDialectURI(str, Enum):
    """URIs identifying DPWS device metadata dialects."""

    THIS_MODEL = 'http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/ThisModel'
    THIS_DEVICE = 'http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/ThisDevice'
    RELATIONSHIP = 'http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/Relationship'


class DeviceEventingFilterDialectURI(str, Enum):
    """URIs identifying DPWS eventing filter dialects."""

    ACTION = 'http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/Action'


class Relationship:
    """Container for a host service and its hosted services."""

    __slots__ = ('host', 'hosted')

    def __init__(self):
        self.host = None
        self.hosted = {}


class LocalizedStringTypeDict(dict):
    """Represent LocalizedStringType elements as a dictionary of lang:string entries.

    If lang is None, the_string is the default string.
    """

    def add_localized_string(self, the_string: str, lang: str | None = None) -> None:
        """Add a localized string for the given language.

        :param the_string: the localized text
        :param lang: the language of the text, or None for the default string
        :return: None
        """
        self[lang] = the_string


class HostServiceType(XMLTypeBase):
    """DPWS HostServiceType describing the hosting service of a device."""

    EndpointReference = cp.SubElementProperty(
        default_ns_helper.WSA.tag('EndpointReference'), value_class=EndpointReferenceType
    )
    Types = cp.NodeTextQNameListProperty(default_ns_helper.DPWS.tag('Types'))
    _props = ('EndpointReference', 'Types')


class HostedServiceType(XMLTypeBase):
    """DPWS HostedServiceType describing a service hosted by a device."""

    EndpointReference = cp.SubElementListProperty(
        default_ns_helper.WSA.tag('EndpointReference'), value_class=EndpointReferenceType
    )
    Types = cp.NodeTextQNameListProperty(default_ns_helper.DPWS.tag('Types'))
    ServiceId = cp.AnyUriTextElement(default_ns_helper.DPWS.tag('ServiceId'))
    _props = ('EndpointReference', 'Types', 'ServiceId')


class LocalizedStringType(ElementWithText):
    """DPWS LocalizedStringType: a text element with an optional language attribute."""

    lang = cp.StringAttributeProperty(default_ns_helper.XML.tag('lang'))
    _props = ('lang',)

    @classmethod
    def init(cls, text: str, lang: str | None = None) -> LocalizedStringType:
        """Create a LocalizedStringType instance.

        :param text: the text
        :param lang: if given, the actual language
        :return: the created instance
        """
        instance = cls()
        instance.lang = lang
        instance.text = text
        return instance


class ThisDeviceType(XMLTypeBase):
    """DPWS ThisDeviceType metadata (friendly name, firmware and serial number)."""

    FriendlyName = cp.SubElementListProperty(
        default_ns_helper.DPWS.tag('FriendlyName'), value_class=LocalizedStringType
    )
    FirmwareVersion = cp.NodeStringProperty(default_ns_helper.DPWS.tag('FirmwareVersion'), is_optional=True)
    SerialNumber = cp.NodeStringProperty(default_ns_helper.DPWS.tag('SerialNumber'), is_optional=True)
    _props = ('FriendlyName', 'FirmwareVersion', 'SerialNumber')

    def __init__(
        self,
        friendly_name: str | LocalizedStringTypeDict | None = None,
        firmware_version: str | None = None,
        serial_number: str | None = None,
    ):
        """Construct a ThisDeviceType.

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
    """DPWS ThisModelType metadata (manufacturer and model information)."""

    Manufacturer = cp.SubElementListProperty(
        default_ns_helper.DPWS.tag('Manufacturer'), value_class=LocalizedStringType
    )
    ManufacturerUrl = cp.NodeStringProperty(default_ns_helper.DPWS.tag('ManufacturerUrl'))
    ModelName = cp.SubElementListProperty(default_ns_helper.DPWS.tag('ModelName'), value_class=LocalizedStringType)
    ModelNumber = cp.NodeStringProperty(default_ns_helper.DPWS.tag('ModelNumber'), is_optional=True)
    ModelUrl = cp.NodeStringProperty(default_ns_helper.DPWS.tag('ModelUrl'), is_optional=True)
    PresentationUrl = cp.NodeStringProperty(default_ns_helper.DPWS.tag('PresentationUrl'), is_optional=True)
    _props = ('Manufacturer', 'ManufacturerUrl', 'ModelName', 'ModelNumber', 'ModelUrl', 'PresentationUrl')

    def __init__(  # noqa: PLR0913 - mirrors the DPWS ThisModelType schema fields; part of public API
        self,
        manufacturer: str | LocalizedStringTypeDict | None = None,
        manufacturer_url: str | None = None,
        model_name: str | LocalizedStringTypeDict | None = None,
        model_number: str | None = None,
        model_url: str | None = None,
        presentation_url: str | None = None,
    ):
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
