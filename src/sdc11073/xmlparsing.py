import copy
import typing

from lxml import etree as etree_

from .definitions_base import ProtocolsRegistry


# class BicepsSchema(object):
#     def __init__(self, definition_cls):
#         """
#
#         :param base_definition_cls: a class derived from BaseDefinitions, it contains paths to xml schema files
#         """
#         self.parser = etree_.ETCompatXMLParser()
#         self._versionRef = definition_cls
#         self.parser.resolvers.add(self._versionRef.schemaResolver)
#
#         pmSchema = self._parseFile(self._versionRef.ParticipantModelSchemaFile)
#         self.pmSchema = etree_.XMLSchema(etree=pmSchema)
#
#         bmmSchema = self._parseFile(self._versionRef.MessageModelSchemaFile)
#         self.bmmSchema = etree_.XMLSchema(etree=bmmSchema)
#
#         mexSchema = self._parseFile(self._versionRef.MetaDataExchangeSchemaFile, normalized=False)
#         self.mexSchema = etree_.XMLSchema(etree=mexSchema)
#
#         evtSchema = self._parseFile(self._versionRef.EventingSchemaFile, normalized=False)
#         self.evtSchema = etree_.XMLSchema(etree=evtSchema)
#
#         s12Schema = self._parseFile(self._versionRef.SoapEnvelopeSchemaFile, normalized=False)
#         self.s12Schema = etree_.XMLSchema(etree=s12Schema)
#
#         dpwsSchema = self._parseFile(self._versionRef.DPWSSchemaFile, normalized=False)
#         self.dpwsSchema = etree_.XMLSchema(etree=dpwsSchema)
#
#     def __str__(self):
#         return '{} {}'.format(self.__class__.__name__, self._versionRef.__name__)
#
#     def _parseFile(self, path, normalized=True):
#         with open(path, 'rb') as f:
#             xml_text = f.read()
#         if normalized:
#             xml_text = self._versionRef.normalizeXMLText(xml_text)
#         return etree_.fromstring(xml_text, parser=self.parser, base_url=path)


def _shortActionString(action):
    for cls in ProtocolsRegistry.protocols:
        if cls.ActionsNamespace is not None and action.startswith(cls.ActionsNamespace):
            return '{}:{}'.format(cls.__name__, action[len(cls.ActionsNamespace):])
    return action


def shortFilterString(actions):
    """
    Helper function to make shorter action strings for logging
    :param actions: list of strings
    :return: a comma separated string of shortened names
    """
    return ', '.join([_shortActionString(a) for a in actions])


def copy_node(node: etree_._Element, method=copy.deepcopy) -> etree_._Element:
    """
    Copy and preserve complete namespace. See https://github.com/Draegerwerk/sdc11073/issues/191

    :param node: node to be copied
    :param method: method that copies an etree element
    :return: new node
    """
    # walk from target to root
    current = node
    ns_map_list: typing.List[typing.Dict[str, str]] = []  # saves all namespaces
    while current is not None:
        ns_map_list.append({k: v for k, v in current.nsmap.items() if k})  # filter for default namespace
        current = current.getparent()

    # create new instance
    root_tree = node.getroottree()
    current = method(root_tree.getroot())
    x_path_steps = root_tree.getpath(node).split('/')[1:]
    assert len(x_path_steps) == len(ns_map_list)

    # walk from root to target
    ns_map_list.reverse()
    for i, step in enumerate(x_path_steps):
        if i == 0:
            step = f'/{step}'
        current = current.xpath(step, namespaces=ns_map_list[i])[0]
    return current


def copy_node_wo_parent(node: etree_._Element, method=copy.deepcopy) -> etree_._Element:
    """
    Copy node but only keep relevant information and no parent.

    :param node: node to be copied
    :param method: method that copies an etree element
    :return: new node
    """
    new_node = etree_.Element(node.tag, attrib=node.attrib, nsmap=node.nsmap)
    new_node.text = node.text
    new_node.tail = node.tail
    new_node.extend((method(child) for child in node))
    return new_node
