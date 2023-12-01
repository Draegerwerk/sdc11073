from __future__ import annotations

import pathlib
import traceback
from io import StringIO
from typing import TYPE_CHECKING
from urllib import parse

from lxml import etree as etree_

from . import loghelper

if TYPE_CHECKING:
    from .namespaces import NamespaceHelper, PrefixNamespace


def mk_schema_validator(namespaces: list[PrefixNamespace], ns_helper: NamespaceHelper) -> etree_.XMLSchema:
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

    def __init__(self, namespaces: list[PrefixNamespace], log_prefix=None):
        super().__init__()
        self.namespaces = namespaces
        self._logger = loghelper.get_logger_adapter('sdc.schema_resolver', log_prefix)

    def resolve(self, system_url, _, context):
        """Look for xml file location in self.namespaces."""
        self._logger.debug('try to resolve %s', system_url)
        try:
            path = self._get_schema_file_path(system_url)
            if path:
                self._logger.debug('could resolve url %s via lookup to %s', system_url, path)
                return self.resolve_string(path.read_bytes(), context, base_url=str(path))
            self._logger.error('no schema file for url "%s": ', system_url)
            return None
        except Exception:
            self._logger.error('error resolving %s: %s', system_url, traceback.format_exc())
            raise

    def _get_schema_file_path(self, url: str) -> pathlib.Path | None:
        return next((entry.local_schema_file for entry in self.namespaces if entry.schema_location_url == url), None)
