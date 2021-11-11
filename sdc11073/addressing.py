from typing import Optional, Any
import uuid

class EndpointReferenceType:
    """ Acc. to "http://www.w3.org/2005/08/addressing"

    """
    __slots__ = ('address', 'reference_properties', 'reference_parameters', 'port_type', 'service_name')

    def __init__(self, address: str, reference_properties: Optional[Any]=None, reference_parameters: Optional[Any]=None,
                 port_type: Optional[Any]=None, service_name: Optional[Any]=None):
        """

        :param address: wsa:AttributedURI
        :param reference_properties: wsa:ReferencePropertiesType
        :param reference_parameters:  wsa:ReferenceParametersType
        :param port_type: wsa:AttributedQName
        :param service_name: wsa:ServiceNameType
        """
        self.address = address  # type="wsa:AttributedURI", which is an xs:anyURI element
        self.reference_properties = reference_properties
        self.reference_parameters = reference_parameters
        self.port_type = port_type
        self.service_name = service_name

    def __str__(self):
        return f'{self.__class__.__name__}: address={self.address}'


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
        self.reference_parameters = reference_parameters
        self.relationship_type = relationship_type

    def mk_reply_address(self, action):
        return Address(action=action, relates_to=self.message_id)
