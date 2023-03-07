from __future__ import annotations

import uuid
import copy
from typing import Optional, List, TYPE_CHECKING

from sdc11073.namespaces import default_ns_helper as nsh
from . import xml_structure as struct
from .basetypes import XMLTypeBase, ElementWithText

if TYPE_CHECKING:
    from lxml.etree import QName, _Element


class EndpointReferenceType(XMLTypeBase):
    Address = struct.NodeStringProperty(nsh.wsaTag('Address'))
    ReferenceParameters = struct.AnyEtreeNodeListProperty(nsh.wsaTag('ReferenceParameters'), is_optional=True)
    PortType = struct.NodeTextQNameProperty(nsh.wsaTag('PortType'), is_optional=True)
    _props = ['Address', 'ReferenceParameters', 'PortType']


class Relationship(ElementWithText):
    RelationshipType = struct.QNameAttributeProperty('RelationshipType')
    _props = ['RelationshipType']


class MustUnderStandTextElement(ElementWithText):
    _must_understand = struct.BooleanAttributeProperty(nsh.s12Tag('mustUnderstand'), default_py_value=True)
    _props = ['_must_understand']

    def __init__(self, text: Optional[str] = None):
        super().__init__()
        self.text = text


# class Address:
#     """ Acc. to "http://www.w3.org/2005/08/addressing"
#
#     """
#     __slots__ = ('message_id', 'addr_to', 'addr_from', 'reply_to', 'fault_to', 'action',
#                  'relates_to', 'reference_parameters', 'relationship_type')
#
#     def __init__(self, action, message_id=None, addr_to=None, relates_to=None, addr_from=None, reply_to=None,
#                  fault_to=None, reference_parameters=None,
#                  relationship_type=None):  # pylint: disable=too-many-arguments
#         """
#
#         :param action: xs:anyURI string, required
#         :param message_id: xs:anyURI string or None; default is None
#                           if None, a message_id is generated automatically
#         :param addr_to: xs:anyURI string, optional
#         :param relates_to: xs:anyURI string, 0...n
#         :param addr_from: WsaEndpointReferenceType instance, optional
#         :param reply_to: WsaEndpointReferenceType instance, optional
#         :param fault_to: WsaEndpointReferenceType instance, optional
#         :param reference_parameters: a list of something, optional
#         :param relationship_type: a QName, optional
#         """
#         self.action = action
#         self.message_id = message_id or uuid.uuid4().urn
#         self.addr_to = addr_to
#         self.relates_to = relates_to
#         self.addr_from = addr_from
#         self.reply_to = reply_to
#         self.fault_to = fault_to
#         if reference_parameters is not None:
#             self.reference_parameters = reference_parameters
#         else:
#             self.reference_parameters = []
#
#         self.relationship_type = relationship_type
#
#     def mk_reply_address(self, action):
#         return Address(action=action, relates_to=self.message_id)


class HeaderInformationBlock(XMLTypeBase):
    MessageID = struct.AnyUriTextElement(nsh.wsaTag('MessageID'))
    RelatesTo = struct.SubElementProperty(nsh.wsaTag('RelatesTo'),
                                          value_class=Relationship,
                                          is_optional=True)
    To = struct.SubElementProperty(nsh.wsaTag('To'),
                                   value_class=MustUnderStandTextElement,
                                   is_optional=True)
    Action = struct.SubElementProperty(nsh.wsaTag('Action'),
                                       value_class=MustUnderStandTextElement,
                                       is_optional=True)
    From = struct.SubElementProperty(nsh.wsaTag('From'),
                                     value_class=EndpointReferenceType,
                                     is_optional=True)
    _props = ['MessageID', 'RelatesTo', 'To', 'Action', 'From']

    def __init__(self, action: Optional[str] = None,
                 message_id: Optional[str] = None,
                 addr_to: Optional[str] = None,
                 relates_to: Optional[str] = None,
                 addr_from: Optional[str] = None,
                 reference_parameters:Optional[List[_Element]] = None,
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
        self.RelatesTo = relates_to
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


