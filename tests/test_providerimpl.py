"""Unit tests for provider implementation."""

import pathlib
import unittest
from typing import Any
from unittest import mock

from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.dispatch import DispatchKey
from sdc11073.mdib import ProviderMdib
from sdc11073.namespaces import EventingActions, PrefixNamespace
from sdc11073.namespaces import default_ns_helper as ns_hlp
from sdc11073.provider.providerimpl import SdcProvider, provider_components_async_factory

MDIB_FOLDER = pathlib.Path(__file__).parent


class TestProviderHttpServerTimeout(unittest.TestCase):
    """Test HTTP server timeout error message formatting."""

    def setUp(self):
        """Set up test fixtures."""
        self.mock_mdib = ProviderMdib.from_mdib_file(
            str(MDIB_FOLDER.joinpath('70041_MDIB_Final.xml')),
            protocol_definition=SdcV1Definitions,
        )

    def test_not_started_http_server_exception(self):
        """Test that a non-started HTTP server causes a RuntimeError."""
        provider = SdcProvider(
            ws_discovery=mock.MagicMock(),
            this_model=mock.MagicMock(),
            this_device=mock.MagicMock(),
            device_mdib_container=self.mock_mdib,
            epr='urn:uuid:test-device',
        )

        class _DummyHttpServerThread:
            def __init__(self, *_: Any, **__: Any):
                self.started_evt = mock.MagicMock()
                self.started_evt.wait.return_value = False

            def start(self):
                pass

            def run(self):
                pass

        with mock.patch('sdc11073.provider.providerimpl.HttpServerThreadBase', new=_DummyHttpServerThread):
            with self.assertRaises(RuntimeError) as err:
                provider._start_services(shared_http_server=None)

            expected_msg = 'Http server could not be started within 60.0 seconds.'
            self.assertEqual(str(err.exception), expected_msg)


class TestSchemaSpecsPassedToMsgFactory(unittest.TestCase):
    """Test that port type additional_namespaces are passed correctly."""

    def test_msg_factory_receives_port_type_namespaces(self):
        """Verify additional_namespaces from port types is processed correctly."""
        extra_ns = PrefixNamespace('tst', 'https://example.com/test-namespace', 'test.xsd', None)

        mock_port_type = mock.MagicMock()
        mock_port_type.additional_namespaces = [extra_ns]

        components = provider_components_async_factory()
        components.hosted_services = {'test_service': [mock_port_type]}
        components.additional_schema_specs = set()
        components.services_factory = mock.MagicMock()  # avoid service factory calls during test

        # Use mocks to capture the schema specs arguments.
        # __deepcopy__ returns itself so the mock survives a deepcopy.
        mock_factory_cls = mock.Mock()
        mock_factory_cls.__deepcopy__ = lambda _: mock_factory_cls
        mock_reader_cls = mock.Mock()
        mock_reader_cls.__deepcopy__ = lambda _: mock_reader_cls
        components.msg_factory_class = mock_factory_cls
        components.msg_reader_class = mock_reader_cls

        mdib = ProviderMdib.from_mdib_file(
            str(MDIB_FOLDER.joinpath('70041_MDIB_Final.xml')),
            protocol_definition=SdcV1Definitions,
        )

        SdcProvider(
            ws_discovery=mock.MagicMock(),
            this_model=mock.MagicMock(),
            this_device=mock.MagicMock(),
            device_mdib_container=mdib,
            epr='urn:uuid:test-device',
            components=components,
        )

        # The second positional arg to both classes is the additional_schema_specs list
        self.assertEqual(1, mock_reader_cls.call_count, 'msg_reader should be instantiated once')
        self.assertEqual(1, mock_factory_cls.call_count, 'msg_factory should be instantiated once')

        self.assertIn(extra_ns, mock_reader_cls.call_args[0][1], 'msg_reader should receive port type namespaces')
        self.assertIn(extra_ns, mock_factory_cls.call_args[0][1], 'msg_factory should receive port type namespaces')


class TestEventingHandlersRegistration(unittest.TestCase):
    """Hosts without a subscriptions manager must not advertise eventing handlers.

    Regression: a Subscribe POST to such a host previously raised
    ``AttributeError: 'NoneType' object has no attribute 'on_subscribe_request'``.
    """

    _EVENTING_KEYS = (
        DispatchKey(EventingActions.Subscribe, ns_hlp.WSE.tag('Subscribe')),
        DispatchKey(EventingActions.Unsubscribe, ns_hlp.WSE.tag('Unsubscribe')),
        DispatchKey(EventingActions.GetStatus, ns_hlp.WSE.tag('GetStatus')),
        DispatchKey(EventingActions.Renew, ns_hlp.WSE.tag('Renew')),
    )

    def setUp(self):
        self.mdib = ProviderMdib.from_mdib_file(
            str(MDIB_FOLDER.joinpath('70041_MDIB_Final.xml')),
            protocol_definition=SdcV1Definitions,
        )
        self.provider = SdcProvider(
            ws_discovery=mock.MagicMock(),
            this_model=mock.MagicMock(),
            this_device=mock.MagicMock(),
            device_mdib_container=self.mdib,
            epr='urn:uuid:test-device',
            components=provider_components_async_factory(),
        )

    def test_hosts_without_subscriptions_manager_do_not_register_eventing(self):
        for host_name in ('Get', 'ContainmentTree'):
            hosted = self.provider.hosted_services.dpws_hosted_services[host_name]
            self.assertIsNone(
                hosted.subscriptions_manager,
                f'precondition: {host_name} host should have no subscriptions manager',
            )
            for key in self._EVENTING_KEYS:
                self.assertNotIn(
                    key,
                    hosted._post_handlers,
                    f'{host_name} host must not register {key.action} when subscriptions_manager is None',
                )

    def test_hosts_with_subscriptions_manager_still_register_eventing(self):
        for host_name in ('StateEvent', 'Set'):
            hosted = self.provider.hosted_services.dpws_hosted_services[host_name]
            self.assertIsNotNone(
                hosted.subscriptions_manager,
                f'precondition: {host_name} host should have a subscriptions manager',
            )
            for key in self._EVENTING_KEYS:
                self.assertIn(
                    key,
                    hosted._post_handlers,
                    f'{host_name} host must register {key.action} when subscriptions_manager is set',
                )
