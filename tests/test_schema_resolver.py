"""Tests for the SchemaResolver class."""

from unittest import TestCase, mock

from lxml import etree

from sdc11073 import namespaces, schema_resolver
from sdc11073.namespaces import PrefixesEnum
from sdc11073.schema_resolver import SchemaResolver


class TestSchemaResolver(TestCase):
    def test_resolver(self):
        resolver = SchemaResolver(PrefixesEnum)

        # verify that a proper call returns an _InputDocument
        result = resolver.resolve(PrefixesEnum.MSG.schema_location_url, None, None)
        self.assertEqual(result.__class__.__name__, '_InputDocument')

        # verify that a call with an unknown schema locations returns None
        result = resolver.resolve('foobar', None, None)
        self.assertIsNone(result)

        # verify that resolve raises an Exception if something unexpected happens
        resolver = SchemaResolver([1, 2, 3])
        self.assertRaises(AttributeError, resolver.resolve, 'foobar', None, None)


class TestMkSchemaValidatorRetry(TestCase):
    """Cover the retry loop that works around the process global libxml2 entity loader."""

    def setUp(self):
        self._real_xml_schema = etree.XMLSchema
        sleep_patcher = mock.patch.object(schema_resolver.time, 'sleep')
        self.sleep = sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)

    def _mk_schema_validator(self) -> etree.XMLSchema:
        return schema_resolver.mk_schema_validator(list(namespaces.PrefixesEnum), namespaces.default_ns_helper)

    def test_retry_after_parse_error(self):
        valid_schema = self._mk_schema_validator()
        side_effect = [etree.XMLSchemaParseError('simulated'), valid_schema]
        with mock.patch.object(schema_resolver.etree, 'XMLSchema', side_effect=side_effect):
            schema = self._mk_schema_validator()
        self.assertIs(schema, valid_schema)
        self.sleep.assert_called_once_with(schema_resolver._SCHEMA_RETRY_DELAYS[0])

    def test_retry_after_unlocated_imports(self):
        with mock.patch.object(schema_resolver, '_has_unlocated_imports', side_effect=[True, True, False]):
            schema = self._mk_schema_validator()
        self.assertIsInstance(schema, self._real_xml_schema)
        self.assertEqual(
            [c.args[0] for c in self.sleep.call_args_list],
            list(schema_resolver._SCHEMA_RETRY_DELAYS[:2]),
        )

    def test_final_attempt_succeeds(self):
        retries = len(schema_resolver._SCHEMA_RETRY_DELAYS)
        with mock.patch.object(schema_resolver, '_has_unlocated_imports', side_effect=[True] * retries + [False]):
            schema = self._mk_schema_validator()
        self.assertIsInstance(schema, self._real_xml_schema)
        self.assertEqual(self.sleep.call_count, retries)

    def test_raises_if_imports_stay_unlocated(self):
        with (
            mock.patch.object(schema_resolver, '_has_unlocated_imports', return_value=True),
            self.assertRaises(etree.XMLSchemaParseError) as ctx,
        ):
            self._mk_schema_validator()
        self.assertIn('could not locate imported schemas', str(ctx.exception))
        self.assertEqual(self.sleep.call_count, len(schema_resolver._SCHEMA_RETRY_DELAYS))

    def test_raises_if_parse_error_persists(self):
        error = etree.XMLSchemaParseError('simulated')
        with (
            mock.patch.object(schema_resolver.etree, 'XMLSchema', side_effect=error),
            self.assertRaises(etree.XMLSchemaParseError) as ctx,
        ):
            self._mk_schema_validator()
        self.assertIs(ctx.exception, error)
        self.assertEqual(self.sleep.call_count, len(schema_resolver._SCHEMA_RETRY_DELAYS))

    def test_has_unlocated_imports(self):
        schema = self._mk_schema_validator()
        self.assertFalse(schema_resolver._has_unlocated_imports(schema))
        unlocated = etree.XMLSchema(
            etree.fromstring(
                '<xsd:schema xmlns:xsd="http://www.w3.org/2001/XMLSchema">'
                '<xsd:import namespace="urn:foo" schemaLocation="does_not_exist.xsd"/>'
                '</xsd:schema>'
            )
        )
        self.assertTrue(schema_resolver._has_unlocated_imports(unlocated))
