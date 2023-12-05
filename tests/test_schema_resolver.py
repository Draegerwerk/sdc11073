from unittest import TestCase
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
        resolver = SchemaResolver([ 1, 2, 3])
        self.assertRaises(AttributeError, resolver.resolve, 'foobar', None, None)

