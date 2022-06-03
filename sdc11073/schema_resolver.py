import os
import traceback
from urllib import parse

from lxml import etree as etree_

from . import loghelper
from .namespaces import Prefix_Namespace as Prefixes


def mk_schema_validator(schema_resolver: etree_.Resolver) -> etree_.XMLSchema:
    parser = etree_.XMLParser(resolve_entities=True)
    parser.resolvers.add(schema_resolver)
    # create a schema that includes all used schemas into a single one
    all_included = f'''<?xml version="1.0" encoding="UTF-8"?>
    <xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema" elementFormDefault="qualified">
     <xsd:import namespace="http://www.w3.org/2003/05/soap-envelope" schemaLocation="http://www.w3.org/2003/05/soap-envelope"/>
     <xsd:import namespace="http://schemas.xmlsoap.org/ws/2004/08/eventing" schemaLocation="http://schemas.xmlsoap.org/ws/2004/08/eventing"/>
     <xsd:import namespace="http://schemas.xmlsoap.org/ws/2004/09/mex" schemaLocation="http://schemas.xmlsoap.org/ws/2004/09/mex"/>
     <xsd:import namespace="http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01" schemaLocation="http://docs.oasis-open.org/ws-dd/discovery/1.1/os/wsdd-discovery-1.1-schema-os.xsd"/>
     <xsd:import namespace="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01" schemaLocation="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01"/>
     <xsd:import namespace="http://www.w3.org/2005/08/addressing" schemaLocation="http://www.w3.org/2006/03/addressing/ws-addr.xsd"/>
     <xsd:import namespace="http://schemas.xmlsoap.org/wsdl/" schemaLocation="http://schemas.xmlsoap.org/wsdl/"/>
     <xsd:import namespace="{Prefixes.MSG.namespace}" schemaLocation="http://standards.ieee.org/downloads/11073/11073-10207-2017/BICEPS_MessageModel.xsd"/>
     </xsd:schema>'''.encode('utf-8')

    elem_tree = etree_.fromstring(all_included, parser=parser, base_url='C://')
    return etree_.XMLSchema(etree=elem_tree)


def _needs_normalize(filename):
    return filename.endswith('ExtensionPoint.xsd') or \
           filename.endswith('BICEPS_ParticipantModel.xsd') or \
           filename.endswith('BICEPS_MessageModel.xsd')


class SchemaResolver(etree_.Resolver):

    def __init__(self, base_definitions, log_prefix=None):
        super().__init__()
        self._base_definitions = base_definitions
        self._logger = loghelper.getLoggerAdapter('sdc.schema_resolver', log_prefix)

    def resolve(self, url, id, context):  # pylint: disable=unused-argument, redefined-builtin, invalid-name
        try:
            return self._resolve(url, id, context)
        except:
            self._logger.error(traceback.format_exc())

    def _resolve(self, url, id, context):  # pylint: disable=unused-argument, redefined-builtin, invalid-name
        # first check if there is a lookup defined
        self._logger.debug('try to resolve {}', url)
        path = self._base_definitions.get_schema_file_path(url)
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
            path = parse.unquote(path)  # url decode path

        if not os.path.exists(path):
            self._logger.error('no schema file for url "{}": resolved to "{}", but file does not exist', url, path)
            return None
        with open(path, 'rb') as my_file:
            xml_text = my_file.read()
        if _needs_normalize(path):
            xml_text = self._base_definitions.normalizeXMLText(xml_text)
        return self.resolve_string(xml_text, context, base_url=path)

