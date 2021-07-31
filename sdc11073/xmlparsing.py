from lxml import etree as etree_

from .definitions_base import ProtocolsRegistry

class BicepsSchema:
    def __init__(self, definition_cls):
        '''

        :param base_definition_cls: a class derived from BaseDefinitions, it contains paths to xml schema files
        '''
        self.parser = etree_.ETCompatXMLParser(resolve_entities=False)
        self._version_ref = definition_cls
        self.parser.resolvers.add(self._version_ref.schemaResolver)

        participant_schema = self._parse_file(self._version_ref.ParticipantModelSchemaFile)
        self.participant_schema = etree_.XMLSchema(etree=participant_schema)

        message_schema = self._parse_file(self._version_ref.MessageModelSchemaFile)
        self.message_schema = etree_.XMLSchema(etree=message_schema)

        mex_schema = self._parse_file(self._version_ref.MetaDataExchangeSchemaFile, normalized=False)
        self.mex_schema = etree_.XMLSchema(etree=mex_schema)

        eventing_schema = self._parse_file(self._version_ref.EventingSchemaFile, normalized=False)
        self.eventing_schema = etree_.XMLSchema(etree=eventing_schema)

        soap12_schema = self._parse_file(self._version_ref.SoapEnvelopeSchemaFile, normalized=False)
        self.soap12_schema = etree_.XMLSchema(etree=soap12_schema)

        dpws_schema = self._parse_file(self._version_ref.DPWSSchemaFile, normalized=False)
        self.dpws_schema = etree_.XMLSchema(etree=dpws_schema)

    def __str__(self):
        return '{} {}'.format(self.__class__.__name__, self._version_ref.__name__)

    def _parse_file(self, path, normalized=True):
        with open(path, 'rb') as _file:
            xml_text = _file.read()
        if normalized:
            xml_text = self._version_ref.normalize_xml_text(xml_text)
        return etree_.fromstring(xml_text, parser=self.parser, base_url=path)


def _short_action_string(action):
    for cls in ProtocolsRegistry.protocols:
        if cls.ActionsNamespace is not None and action.startswith(cls.ActionsNamespace):
            return '{}:{}'.format(cls.__name__, action[len(cls.ActionsNamespace):])
    return action


def short_filter_string(actions):
    '''
    Helper function to make shorter action strings for logging
    :param actions: list of strings
    :return: a comma separated string of shortened names
    '''
    return ', '.join([_short_action_string(a) for a in actions])
