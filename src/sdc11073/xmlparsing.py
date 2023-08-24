import copy

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

    :param node: report node to be copied
    :param method: method that copies an etree element
    :return: new report node
    """
    root_tree = node.getroottree()
    new_report = method(root_tree.getroot())
    ns_map = {k: v for k, v in node.nsmap.items() if k}  # filter for default namespace
    return new_report.xpath(root_tree.getpath(node), namespaces=ns_map)[0]


def compare_xml_extension(left: etree_._Element, right: etree_._Element) -> bool:
    if left.tag != right.tag:  # compare expanded names
        return False
    if dict(left.attrib) != dict(right.attrib):  # unclear how lxml _Attrib compares
        return False

    # ignore comments
    left_children = [child for child in left if not isinstance(child, etree_._Comment)]
    right_children = [child for child in right if not isinstance(child, etree_._Comment)]

    if len(left_children) != len(right_children):  # compare children count
        return False
    if len(left_children) == 0 and len(right_children) == 0:
        if left.text != right.text:  # mixed content is not allowed. only compare text if there are no children
            return False

    return all(map(compare_xml_extension, left_children, right_children))  # compare children but keep order
