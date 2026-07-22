"""Tests for PathElementRegistry."""

from unittest import TestCase

from sdc11073.dispatch.pathelementregistry import PathElementRegistry
from sdc11073.exceptions import ApiUsageError, InvalidPathError
from sdc11073.pysoap.soapenvelope import Fault


class TestPathElementRegistry(TestCase):
    """Tests for PathElementRegistry."""

    def setUp(self):
        self.registry = PathElementRegistry()

    def test_register_and_get_instance(self):
        """Verify that a registered instance can be retrieved by its path element."""
        instance = object()
        self.registry.register_instance('foo', instance)
        self.assertIs(self.registry.get_instance('foo'), instance)

    def test_register_none_path_element(self):
        """Verify that None is a valid path element."""
        instance = object()
        self.registry.register_instance(None, instance)
        self.assertIs(self.registry.get_instance(None), instance)

    def test_register_duplicate_raises(self):
        """Verify that registering the same path element twice raises ApiUsageError."""
        self.registry.register_instance('foo', object())
        with self.assertRaises(ApiUsageError):
            self.registry.register_instance('foo', object())

    def test_register_duplicate_keeps_original_instance(self):
        """Verify that a rejected duplicate registration does not overwrite the original."""
        original = object()
        self.registry.register_instance('foo', original)
        with self.assertRaises(ApiUsageError):
            self.registry.register_instance('foo', object())
        self.assertIs(self.registry.get_instance('foo'), original)

    def test_register_distinct_path_elements(self):
        """Verify that distinct path elements are looked up independently."""
        first = object()
        second = object()
        self.registry.register_instance('foo', first)
        self.registry.register_instance('bar', second)
        self.assertIs(self.registry.get_instance('foo'), first)
        self.assertIs(self.registry.get_instance('bar'), second)

    def test_get_unknown_path_element_raises(self):
        """Verify that looking up an unregistered path element raises InvalidPathError."""
        with self.assertRaises(InvalidPathError):
            self.registry.get_instance('unknown')

    def test_get_unknown_path_element_fault(self):
        """Verify that the InvalidPathError carries a sender soap fault and reason."""
        with self.assertRaises(InvalidPathError) as ctx:
            self.registry.get_instance('unknown')
        error = ctx.exception
        self.assertEqual(error.status, 404)
        self.assertIn('unknown', error.reason)
        self.assertIsInstance(error.soap_fault, Fault)
