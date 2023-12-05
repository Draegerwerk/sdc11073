from __future__ import annotations

import pathlib
import time
import traceback
from io import StringIO
from typing import TYPE_CHECKING, Any

from lxml import etree as etree_
from urllib import parse

from . import loghelper

if TYPE_CHECKING:
    from .namespaces import NamespaceHelper, PrefixNamespace


def mk_schema_validator(namespaces: list[PrefixNamespace], ns_helper: NamespaceHelper) -> etree_.XMLSchema:
    """Create a schema validator."""
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
    # for unknown reason creating the schema fails sometimes. repeat up to 3 times.
    try:
        return etree_.XMLSchema(etree=elem_tree)
    except etree_.XMLSchemaParseError:
        time.sleep(0.1)
    try:
        return etree_.XMLSchema(etree=elem_tree)
    except etree_.XMLSchemaParseError:
        time.sleep(0.5)
    return etree_.XMLSchema(etree=elem_tree)


class SchemaResolver(etree_.Resolver):
    """A Resolver that uses a list of PrefixNamespace for resolving."""

    def __init__(self, namespaces: list[PrefixNamespace], log_prefix: str | None = None):
        super().__init__()
        self.namespaces = namespaces
        self._logger = loghelper.get_logger_adapter('sdc.schema_resolver', log_prefix)

    def resolve(self, system_url: str, _: Any, context: Any) -> Any: # return whatever type resolve_string returns
        """Look for xml file location in self.namespaces."""
        self._logger.debug('try to resolve %s', system_url)
        try:
            path = self._get_schema_file_path(system_url)
            if path:
                self._logger.debug('could resolve url %s via lookup to %s', system_url, path)
            else:
                # no lookup, parse url
                parsed = parse.urlparse(system_url)
                path = parsed.path if parsed.scheme == 'file' else system_url
                if path.startswith('/') and path[2] == ':':  # invalid construct like /C:/Temp
                    path = path[1:]
            path = pathlib.Path(path)
            if not path.exists():
                self._logger.error('no schema file for url "%s": resolved to "%s", but file does not exist',
                                   system_url, path)
                return None
            return self.resolve_string(path.read_bytes(), context, base_url=str(path))
        except Exception:
            self._logger.error('error resolving %s: %s', system_url, traceback.format_exc())
            raise

    def _get_schema_file_path(self, url: str) -> pathlib.Path | None:
        return next((entry.local_schema_file for entry in self.namespaces if entry.schema_location_url == url), None)
