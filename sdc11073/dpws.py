from enum import Enum


class DeviceRelationshipTypeURI(str, Enum):
    HOST = "http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/host"


class DeviceMetadataDialectURI(str, Enum):
    THIS_MODEL = "http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/ThisModel"
    THIS_DEVICE = "http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/ThisDevice"
    RELATIONSHIP = "http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/Relationship"


class DeviceEventingFilterDialectURI(str, Enum):
    ACTION = "http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/Action"


class RelationShip:
    __slots__ = ('host', 'hosted')

    def __init__(self):
        self.host = None
        self.hosted = {}


class HostServiceType:
    __slots__ = ('endpoint_references', 'types')

    def __init__(self, endpoint_references_list, types_list):
        """
        :param endpoint_references_list: list of WssEndpointReference instances
        :param types_list: a list of etree.QName instances
        """
        self.endpoint_references = endpoint_references_list
        self.types = types_list

    def __str__(self):
        return f'HostServiceType: endpointReference={self.endpoint_references}, types="{self.types}"'


class HostedServiceType:
    __slots__ = ('endpoint_references', 'types', 'service_id', 'soap_client')

    def __init__(self, endpoint_references_list, types_list, service_id):
        self.endpoint_references = endpoint_references_list
        self.types = types_list  # a list of QNames
        self.service_id = service_id
        self.soap_client = None

    def __str__(self):
        return f'HostedServiceType: endpointReference={self.endpoint_references}, types="{self.types}" ' \
               f'service_id="{self.service_id}"'


class ThisDevice:
    __slots__ = ('friendly_name', 'firmware_version', 'serial_number')

    def __init__(self, friendly_name, firmware_version, serial_number):
        if isinstance(friendly_name, dict):
            self.friendly_name = friendly_name
        else:
            self.friendly_name = {None: friendly_name}  # localized texts
        self.firmware_version = firmware_version
        self.serial_number = serial_number

    def __str__(self):
        return f'ThisDevice: friendly_name={self.friendly_name}, ' \
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


class ThisModel:
    __slots__ = ('manufacturer', 'manufacturer_url', 'model_name', 'model_number', 'model_url', 'presentation_url')

    def __init__(self, manufacturer, manufacturer_url, model_name, model_number, model_url, presentation_url):
        if isinstance(manufacturer, dict):
            self.manufacturer = manufacturer
        else:
            self.manufacturer = {None: manufacturer}  # localized texts
        self.manufacturer_url = manufacturer_url
        if isinstance(model_name, dict):
            self.model_name = model_name
        else:
            self.model_name = {None: model_name}  # localized texts
        self.model_number = model_number
        self.model_url = model_url
        self.presentation_url = presentation_url

    def __str__(self):
        return f'ThisModel: manufacturer={self.manufacturer}, model_name="{self.model_name}", ' \
               f'model_number="{self.model_number}"'

    def __eq__(self, other):
        try:
            for slot in self.__slots__:
                if getattr(self, slot) != getattr(other, slot):
                    return False
            return True
        except AttributeError:
            return False
