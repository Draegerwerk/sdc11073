import uuid
from sdc11073.namespaces import default_ns_helper
from .pmtypes import PropertyBasedPMType
from . import xml_structure as struct


class EndpointReferenceType(PropertyBasedPMType):
    Address = struct.NodeStringProperty(default_ns_helper.wsaTag('Address'))
    ReferenceProperties = struct.AnyEtreeNodeListProperty(default_ns_helper.wsaTag('ReferenceProperties'), is_optional=True)
    ReferenceParameters = struct.AnyEtreeNodeListProperty(default_ns_helper.wsaTag('ReferenceParameters'), is_optional=True)
    PortType = struct.NodeTextQNameProperty(default_ns_helper.wsaTag('PortType'), is_optional=True)
    # ServiceName
    _props = ['Address', 'ReferenceProperties', 'ReferenceParameters', 'PortType']


class Address:
    """ Acc. to "http://www.w3.org/2005/08/addressing"

    """
    __slots__ = ('message_id', 'addr_to', 'addr_from', 'reply_to', 'fault_to', 'action',
                 'relates_to', 'reference_parameters', 'relationship_type')

    def __init__(self, action, message_id=None, addr_to=None, relates_to=None, addr_from=None, reply_to=None,
                 fault_to=None, reference_parameters=None,
                 relationship_type=None):  # pylint: disable=too-many-arguments
        """

        :param action: xs:anyURI string, required
        :param message_id: xs:anyURI string or None; default is None
                          if None, a message_id is generated automatically
        :param addr_to: xs:anyURI string, optional
        :param relates_to: xs:anyURI string, 0...n
        :param addr_from: WsaEndpointReferenceType instance, optional
        :param reply_to: WsaEndpointReferenceType instance, optional
        :param fault_to: WsaEndpointReferenceType instance, optional
        :param reference_parameters: a list of something, optional
        :param relationship_type: a QName, optional
        """
        self.action = action
        self.message_id = message_id or uuid.uuid4().urn
        self.addr_to = addr_to
        self.relates_to = relates_to
        self.addr_from = addr_from
        self.reply_to = reply_to
        self.fault_to = fault_to
        if reference_parameters is not None:
            self.reference_parameters = reference_parameters
        else:
            self.reference_parameters = []

        self.relationship_type = relationship_type

    def mk_reply_address(self, action):
        return Address(action=action, relates_to=self.message_id)
