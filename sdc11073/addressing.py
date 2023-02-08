from typing import Optional
import uuid
import copy

class ReferenceParameters:
    def __init__(self, reference_parameters):
        """

        :param reference_parameters: a list of etree nodes or None
        """
        self._reference_parameter_nodes = reference_parameters if reference_parameters is not None else []

    @property
    def parameters(self):
        return [copy.copy(node) for node in self._reference_parameter_nodes]

    @property
    def has_parameters(self):
        return len(self._reference_parameter_nodes) > 0

    def __repr__(self):
        return f'addressing.{self.__class__.__name__}({self._reference_parameter_nodes})'


class EndpointReferenceType:
    """ Acc. to "http://www.w3.org/2005/08/addressing"

    """
    __slots__ = ('address', 'reference_parameters')

    def __init__(self, address: str, reference_parameters: Optional[ReferenceParameters]=None):
        """

        :param address: wsa:AttributedURIType
        :param reference_parameters:  ReferenceParameters instance
        (MetaData element is not implemented)
        """
        self.address: str = address  # type="wsa:AttributedURI", which is an xs:anyURI element
        self.reference_parameters : ReferenceParameters = reference_parameters or ReferenceParameters(None)

    def __str__(self):
        return f'{self.__class__.__name__}: address={self.address}'


class Address:
    """ Acc. to "http://www.w3.org/2005/08/addressing"

    """
    __slots__ = ('message_id', 'addr_to', 'addr_from', 'reply_to', 'fault_to', 'action',
                 'relates_to', 'reference_parameters', 'relationship_type')

    def __init__(self, action, message_id=None, addr_to=None, relates_to=None, addr_from=None, reply_to=None,
                 fault_to=None, reference_parameters: Optional[ReferenceParameters]=None,
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
            self.reference_parameters = ReferenceParameters(None)

        self.relationship_type = relationship_type

    def mk_reply_address(self, action):
        return Address(action=action, relates_to=self.message_id)
