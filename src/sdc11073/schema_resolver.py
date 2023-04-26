from __future__ import annotations

import os
from io import StringIO
from typing import TYPE_CHECKING, List, Union
from urllib import parse

from lxml import etree as etree_

from . import loghelper

if TYPE_CHECKING:
    from .namespaces import PrefixNamespace, NamespaceHelper


def mk_schema_validator(namespaces: List[PrefixNamespace], ns_helper: NamespaceHelper) -> etree_.XMLSchema:
    schema_resolver = SchemaResolver(namespaces)
    parser = etree_.XMLParser(resolve_entities=True)
    parser.resolvers.add(schema_resolver)
    prefix_enum = ns_helper.prefix_enum
    not_needed = [prefix_enum.XSD]
    # create a schema that includes all used schemas into a single one
    tmp = StringIO()
    tmp.write('<?xml version="1.0" encoding="UTF-8"?>')
    tmp.write(f'<xsd:schema xmlns:xsd="{prefix_enum.XSD.namespace}" elementFormDefault="qualified">\n')
    for entry in namespaces:
        if entry.schema_location_url is not None and entry not in not_needed:
            tmp.write(f'<xsd:import namespace="{entry.namespace}" schemaLocation="{entry.schema_location_url}"/>\n')
    tmp.write('</xsd:schema>')
    all_included = tmp.getvalue().encode('utf-8')

    elem_tree = etree_.fromstring(all_included, parser=parser, base_url='C://')
    return etree_.XMLSchema(etree=elem_tree)


class SchemaResolver(etree_.Resolver):

    def __init__(self, namespaces: List[PrefixNamespace], log_prefix=None):
        super().__init__()
        self.namespaces = namespaces
        self._logger = loghelper.get_logger_adapter('sdc.schema_resolver', log_prefix)

    def resolve(self, url, id, context):  # pylint: disable=unused-argument, redefined-builtin, invalid-name
        # first check if there is a lookup defined
        self._logger.debug('try to resolve {}', url)
        path = self._get_schema_file_path(url)
        if path:
            self._logger.debug('could resolve url {} via lookup to {}', url, path)
        else:
            # no lookup, parse url
            parsed = parse.urlparse(url)
            if parsed.scheme == 'file':
                path = parsed.path  # get the path part
            else:  # the url is a path
                path = url
            if path.startswith('/') and path[2] == ':':  # invalid construct like /C:/Temp
                path = path[1:]

        if not os.path.exists(path):
            self._logger.error('no schema file for url "{}": resolved to "{}", but file does not exist', url, path)
            return None
        with open(path, 'rb') as my_file:
            xml_text = my_file.read()
        return self.resolve_string(xml_text, context, base_url=path)

    def _get_schema_file_path(self, url: str) -> Union[str, None]:
        """

        :param url: url of the schema location
        :return: str or None
        """
        return next((entry.local_schema_file for entry in self.namespaces if entry.schema_location_url == url), None)
