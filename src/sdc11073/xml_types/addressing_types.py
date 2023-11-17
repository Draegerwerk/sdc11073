"""Implementation the types described in ws-addressing core from 2006.

More information can be found at
- https://www.w3.org/TR/2006/REC-ws-addr-core-20060509/
- https://www.w3.org/TR/2006/REC-ws-addr-soap-20060509/.
"""

from __future__ import annotations

import copy
import uuid
from typing import TYPE_CHECKING

from sdc11073.namespaces import default_ns_helper as nsh
from sdc11073.xml_types import xml_structure as struct
from sdc11073.xml_types.basetypes import ElementWithText, XMLTypeBase

if TYPE_CHECKING:
    from lxml.etree import QName

    from sdc11073 import xml_utils

_is_reference_parameter = nsh.WSA.tag('IsReferenceParameter')


class EndpointReferenceType(XMLTypeBase):
    """The wsa:EndpointReferenceType type is used wherever a Web service endpoint is referenced.

    The following describes the contents of this type:
    <wsa:EndpointReference>
        <wsa:Address>xs:anyURI</wsa:Address>
        <wsa:ReferenceParameters>xs:any*</wsa:ReferenceParameters> ?
        <wsa:Metadata>xs:any*</wsa:Metadata>?
    </wsa:EndpointReference>
    """

    Address = struct.AnyUriTextElement(nsh.WSA.tag('Address'))
    ReferenceParameters: list[xml_utils.LxmlElement] | None = (
        struct.AnyEtreeNodeListProperty(nsh.WSA.tag('ReferenceParameters'), is_optional=True))
    Metadata: list[xml_utils.LxmlElement] | None = (
        struct.AnyEtreeNodeListProperty(nsh.WSA.tag('Metadata'), is_optional=True))
    _props = ('Address', 'ReferenceParameters', 'Metadata')


class RelatesTo(ElementWithText):
    """Contributes one abstract [relationship] property value."""

    RelationshipType: str | None = struct.AnyUriTextElement(
        nsh.WSA.tag('RelationshipType'),
        is_optional=True,
        implied_py_value='http://www.w3.org/2005/08/addressing/reply')
    _props = ('RelationshipType',)


class HeaderInformationBlock(XMLTypeBase):
    """HeaderInformationBlock contains data that ws-addressing requires in soap header.

    <wsa:To>xs:anyURI</wsa:To> ?
    <wsa:From>wsa:EndpointReferenceType</wsa:From> ?
    <wsa:ReplyTo>wsa:EndpointReferenceType</wsa:ReplyTo> ?
    <wsa:FaultTo>wsa:EndpointReferenceType</wsa:FaultTo> ?
    <wsa:Action>xs:anyURI</wsa:Action>
    <wsa:MessageID>xs:anyURI</wsa:MessageID> ?
    <wsa:RelatesTo RelationshipType="xs:anyURI"?>xs:anyURI</wsa:RelatesTo> *

    in soap this is xs:any with wsa:IsReferenceParameter='true' as list
    <wsa:ReferenceParameters>xs:any*</wsa:ReferenceParameters> ?
    """

    To: str | None = struct.AnyUriTextElement(nsh.WSA.tag('To'), is_optional=True)
    From: EndpointReferenceType | None = struct.SubElementProperty(nsh.WSA.tag('From'),
                                                                   value_class=EndpointReferenceType,
                                                                   is_optional=True)
    ReplyTo: EndpointReferenceType | None = struct.SubElementProperty(nsh.WSA.tag('ReplyTo'),
                                                                      value_class=EndpointReferenceType,
                                                                      is_optional=True)
    FaultTo: EndpointReferenceType | None = struct.SubElementProperty(nsh.WSA.tag('FaultTo'),
                                                                      value_class=EndpointReferenceType,
                                                                      is_optional=True)
    Action: str = struct.AnyUriTextElement(nsh.WSA.tag('Action'))
    # note: ws-addressing declares MessageId as optional, but it is required for ws-discovery
    MessageID: str | None = struct.AnyUriTextElement(nsh.WSA.tag('MessageID'), is_optional=True)
    # note: following the standard would require RelatesTo to be a list, but ws-discovery requires it to be 0..1
    RelatesTo = struct.SubElementProperty(nsh.WSA.tag('RelatesTo'),
                                          value_class=RelatesTo,
                                          is_optional=True)

    _props = ('To', 'From', 'ReplyTo', 'FaultTo', 'Action', 'MessageID', 'RelatesTo')

    def __init__(self,  # noqa: PLR0913
                 action: str | None = None,
                 message_id: str | None = None,
                 addr_to: str | None = None,
                 relates_to: str | None = None,
                 addr_from: str | None = None,
                 reference_parameters: list[xml_utils.LxmlElement] | None = None,
                 relationship_type: str | None = None):
        super().__init__()
        self.Action = action
        self.MessageID = message_id or uuid.uuid4().urn
        if addr_to is not None:
            self.To = addr_to
        if relates_to is not None:
            self.RelatesTo = RelatesTo()
            self.RelatesTo.text = relates_to
            if relationship_type is not None:
                self.RelatesTo.RelationshipType = relationship_type
        if addr_from is not None:
            self.From = EndpointReferenceType()
            self.From.Address = addr_from
        self.reference_parameters: list[xml_utils.LxmlElement] = reference_parameters or []

    def mk_reply_header_block(self,
                              action: str | None = None,
                              message_id: str | None = None,
                              addr_to: str | None = None) -> HeaderInformationBlock:
        """Create a HeaderInformationBlock with RelatesTo information of self."""
        reply_address = HeaderInformationBlock(action, message_id, addr_to)
        reply_address.RelatesTo = RelatesTo()
        reply_address.RelatesTo.text = self.MessageID
        reply_address.Action = action
        return reply_address

    def as_etree_node(self,
                      q_name: QName, ns_map: dict[str, str],
                      parent_node: xml_utils.LxmlElement | None = None) -> xml_utils.LxmlElement:
        """Create etree Element form instance data."""
        node = super().as_etree_node(q_name, ns_map, parent_node)
        for param in self.reference_parameters:
            tmp = copy.deepcopy(param)
            tmp.set(_is_reference_parameter, 'true')
            node.append(tmp)
        return node

    @classmethod
    def from_node(cls, node: xml_utils.LxmlElement) -> HeaderInformationBlock:
        """Create HeaderInformationBlock from etree element."""
        obj = cls()
        obj.update_from_node(node)
        # collect reference parameter child nodes
        for child in node:
            is_reference_parameter = child.attrib.get(_is_reference_parameter, 'false')
            if is_reference_parameter.lower() == 'true':
                obj.reference_parameters.append(child)
        return obj
