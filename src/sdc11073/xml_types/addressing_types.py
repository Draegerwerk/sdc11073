from __future__ import annotations

import copy
import uuid
from typing import TYPE_CHECKING

from sdc11073.namespaces import default_ns_helper as nsh

from . import xml_structure as struct
from .basetypes import ElementWithText, XMLTypeBase

if TYPE_CHECKING:
    from lxml.etree import QName
    from sdc11073 import xml_utils

_is_reference_parameter = nsh.WSA.tag('IsReferenceParameter')


class EndpointReferenceType(XMLTypeBase):
    """EndpointReferenceType of ws-addressing."""

    Address = struct.NodeStringProperty(nsh.WSA.tag('Address'))
    ReferenceParameters = struct.AnyEtreeNodeListProperty(nsh.WSA.tag('ReferenceParameters'), is_optional=True)
    PortType = struct.NodeTextQNameProperty(nsh.WSA.tag('PortType'), is_optional=True)
    _props = ('Address', 'ReferenceParameters', 'PortType')


class Relationship(ElementWithText):
    """Relationship type of ws-addressing."""

    RelationshipType = struct.QNameAttributeProperty('RelationshipType')
    _props = ('RelationshipType',)


class MustUnderStandTextElement(ElementWithText):
    """XML Element with text and mustUnderstand attribute."""

    _must_understand = struct.BooleanAttributeProperty(nsh.S12.tag('mustUnderstand'), default_py_value=True)
    _props = ('_must_understand',)

    def __init__(self, text: str | None = None):
        super().__init__()
        self.text = text


class HeaderInformationBlock(XMLTypeBase):
    """HeaderInformationBlock contains data that ws-addressing requires in soap header."""

    MessageID = struct.AnyUriTextElement(nsh.WSA.tag('MessageID'))
    RelatesTo = struct.SubElementProperty(nsh.WSA.tag('RelatesTo'),
                                          value_class=Relationship,
                                          is_optional=True)
    To = struct.SubElementProperty(nsh.WSA.tag('To'),
                                   value_class=MustUnderStandTextElement,
                                   is_optional=True)
    Action = struct.SubElementProperty(nsh.WSA.tag('Action'),
                                       value_class=MustUnderStandTextElement,
                                       is_optional=True)
    From = struct.SubElementProperty(nsh.WSA.tag('From'),
                                     value_class=EndpointReferenceType,
                                     is_optional=True)
    _props = ('MessageID', 'RelatesTo', 'To', 'Action', 'From')

    def __init__(self, action: str | None = None,
                 message_id: str | None = None,
                 addr_to: str | None = None,
                 relates_to: str | None = None,
                 addr_from: str | None = None,
                 reference_parameters: list[xml_utils.LxmlElement] | None = None,
                 relationship_type: QName | None = None):
        super().__init__()
        if action is not None:
            self.Action = MustUnderStandTextElement(action)
        self.MessageID = message_id or uuid.uuid4().urn
        if addr_to is not None:
            self.To = MustUnderStandTextElement(addr_to)
        if relates_to is not None:
            self.RelatesTo = Relationship()
            self.RelatesTo.text = relates_to
            if relationship_type is not None:
                self.RelatesTo.RelationshipType = relationship_type
        self.From = addr_from
        if reference_parameters is not None:
            self.reference_parameters = reference_parameters
        else:
            self.reference_parameters = []

    @property
    def action(self) -> str | None:
        if self.Action is None:
            return None
        return self.Action.text

    def set_to(self, to: str):
        """Set To element in Soap header."""
        self.To = MustUnderStandTextElement(to)

    def mk_reply_header_block(self, action: str | None = None,
                              message_id: str | None = None,
                              addr_to: str | None = None) -> HeaderInformationBlock:
        """Create a HeaderInformationBlock with RelatesTo information of self."""
        reply_address = HeaderInformationBlock(action, message_id, addr_to)
        reply_address.RelatesTo = Relationship()
        reply_address.RelatesTo.text = self.MessageID
        reply_address.Action = MustUnderStandTextElement(action)
        return reply_address

    def as_etree_node(self, q_name: QName, ns_map: dict[str, str]) -> xml_utils.LxmlElement:
        """Create etree Element form instance data."""
        node = super().as_etree_node(q_name, ns_map)
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
