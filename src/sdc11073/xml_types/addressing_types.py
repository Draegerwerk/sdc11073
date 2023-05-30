from __future__ import annotations

import copy
import uuid
from typing import Optional, List, TYPE_CHECKING

from sdc11073.namespaces import default_ns_helper as nsh
from . import xml_structure as struct
from .basetypes import XMLTypeBase, ElementWithText

if TYPE_CHECKING:
    from lxml.etree import QName, Element


class EndpointReferenceType(XMLTypeBase):
    Address = struct.NodeStringProperty(nsh.WSA.tag('Address'))
    ReferenceParameters = struct.AnyEtreeNodeListProperty(nsh.WSA.tag('ReferenceParameters'), is_optional=True)
    PortType = struct.NodeTextQNameProperty(nsh.WSA.tag('PortType'), is_optional=True)
    _props = ['Address', 'ReferenceParameters', 'PortType']


class Relationship(ElementWithText):
    RelationshipType = struct.QNameAttributeProperty('RelationshipType')
    _props = ['RelationshipType']


class MustUnderStandTextElement(ElementWithText):
    _must_understand = struct.BooleanAttributeProperty(nsh.S12.tag('mustUnderstand'), default_py_value=True)
    _props = ['_must_understand']

    def __init__(self, text: Optional[str] = None):
        super().__init__()
        self.text = text


class HeaderInformationBlock(XMLTypeBase):
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
    _props = ['MessageID', 'RelatesTo', 'To', 'Action', 'From']

    def __init__(self, action: Optional[str] = None,
                 message_id: Optional[str] = None,
                 addr_to: Optional[str] = None,
                 relates_to: Optional[str] = None,
                 addr_from: Optional[str] = None,
                 reference_parameters: Optional[List[Element]] = None,
                 relationship_type: Optional[QName] = None):
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
    def action(self):
        if self.Action is None:
            return None
        return self.Action.text

    def set_to(self, to):
        self.To = MustUnderStandTextElement(to)

    def mk_reply_header_block(self, action: Optional[str] = None,
                              message_id: Optional[str] = None,
                              addr_to: Optional[str] = None) -> HeaderInformationBlock:
        reply_address = HeaderInformationBlock(action, message_id, addr_to)
        reply_address.RelatesTo = Relationship()
        reply_address.RelatesTo.text = self.MessageID
        reply_address.Action = MustUnderStandTextElement(action)
        return reply_address

    def as_etree_node(self, q_name, ns_map):
        """ special handling of reference parameters"""
        node = super().as_etree_node(q_name, ns_map)
        for param in self.reference_parameters:
            tmp = copy.deepcopy(param)
            tmp.set('IsReferenceParameter', 'true')
            node.append(tmp)
        return node

    @classmethod
    def from_node(cls, node):
        """ special handling of reference parameters"""
        obj = cls()
        obj.update_from_node(node)
        # collect reference parameter child nodes
        for child in node:
            is_reference_parameter = child.attrib.get('IsReferenceParameter', 'false')
            if is_reference_parameter.lower() == 'true':
                obj.reference_parameters.append(child)
        return obj
