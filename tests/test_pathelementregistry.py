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

    def test_unregister_instance(self):
        """Verify that an unregistered path element is no longer known."""
        self.registry.register_instance('foo', object())
        self.registry.unregister_instance('foo')
        with self.assertRaises(InvalidPathError):
            self.registry.get_instance('foo')

    def test_unregister_unknown_path_element_is_ignored(self):
        """Verify that unregistering an unknown path element is not an error."""
        self.registry.unregister_instance('unknown')  # must not raise

    def test_unregister_none_path_element(self):
        """Verify that None can be unregistered."""
        self.registry.register_instance(None, object())
        self.registry.unregister_instance(None)
        with self.assertRaises(InvalidPathError):
            self.registry.get_instance(None)

    def test_register_after_unregister(self):
        """Verify that a path element can be registered again after it was unregistered.

        This is needed if a consumer is restarted while it uses a shared http server.
        """
        self.registry.register_instance('foo', object())
        self.registry.unregister_instance('foo')
        second = object()
        self.registry.register_instance('foo', second)  # must not raise ApiUsageError
        self.assertIs(self.registry.get_instance('foo'), second)

    def test_unregister_keeps_other_path_elements(self):
        """Verify that unregistering one path element does not affect the others."""
        first = object()
        second = object()
        self.registry.register_instance('foo', first)
        self.registry.register_instance('bar', second)
        self.registry.unregister_instance('foo')
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
