"""Integration tests for msg:InvocationSource of OperationInvokedReport (IEEE 11073-20701 R0077/R0078).

Verifies end to end that an operation invoked by a consumer that authenticated with a client
certificate is reported with the PEM encoded certificate (default) or, if the provider is configured so, with the
certificate Common Name (R0078), while an operation invoked over an unauthenticated connection is reported with the
anonymous instance identifier (R0077).
"""

from __future__ import annotations

import logging
import pathlib
import ssl
import unittest
from typing import TYPE_CHECKING
from unittest import mock

from tutorial.codedvaluecomparator import _coded_value_comparator

from sdc11073 import certloader, loghelper
from sdc11073.consumer.consumerimpl import SdcConsumer, default_components_factory
from sdc11073.dispatch import RequestDispatcher
from sdc11073.mdib.mdibaccessor import get_one_descriptor_by_type
from sdc11073.provider import invocationsource
from sdc11073.provider.operations import ExecuteParameters, ExecuteResult, SetStringOperation
from sdc11073.provider.providerimpl import SdcProviderComponents, provider_components_async_factory
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_types import msg_types, pm_types
from tests import utils
from tests.mockstuff import SomeDevice

if TYPE_CHECKING:
    from sdc11073.consumer.operations import OperationResult

_CERT_FOLDER = pathlib.Path(__file__).parent.joinpath('certificates')
_CERT_PASSWORD = 'password'  # noqa: S105 - password of the checked-in test key
_CERT_COMMON_NAME = 'CName'  # Common Name of tests/certificates/test_certificate.pem
_SET_TIMEOUT = 10
_MDIB_NAME = 'mdib_single_mds.xml'


def _mk_mutual_tls_context() -> certloader.SSLContextContainer:
    """Build an ssl context container that requires client authentication.

    The self-signed test certificate acts as its own certificate authority, so the same certificate
    is used as identity and as trust anchor for both peers.
    """
    return certloader.mk_ssl_contexts(
        key_file=_CERT_FOLDER.joinpath('test_private_key.pem'),
        cert_file=_CERT_FOLDER.joinpath('test_certificate.pem'),
        ca_file=_CERT_FOLDER.joinpath('test_certificate.pem'),
        ssl_passwd=_CERT_PASSWORD,
    )


class TestOperationInvocationSource(unittest.TestCase):
    def setUp(self):
        loghelper.basic_logging_setup()
        self._logger = logging.getLogger('sdc.test')
        self._logger.info('############### start setUp %s ##############', self._testMethodName)
        self.ssl_context_container = _mk_mutual_tls_context()
        self.wsd = WSDiscovery('127.0.0.1', multicast_port=utils.wsd_port())
        self.wsd.start()
        self.sdc_device = None
        self.sdc_client = None
        self._logger.info('############### setUp done %s ##############', self._testMethodName)

    def tearDown(self):
        self._logger.info('############### tearDown %s ... ##############', self._testMethodName)
        if self.sdc_client is not None:
            self.sdc_client.stop_all()
        if self.sdc_device is not None:
            self.sdc_device.stop_all()
        self.wsd.stop()

    def _invoke_set_string(
        self,
        *,
        use_ssl: bool,
        components: SdcProviderComponents | None = None,
        operation_handle: str | None = None,
        expected_state: msg_types.InvocationState = msg_types.InvocationState.FINISHED,
    ) -> OperationResult:
        """Start a provider and a consumer, invoke a SetString operation and return its result."""
        ssl_context_container = self.ssl_context_container if use_ssl else None
        self.sdc_device = SomeDevice.from_mdib_file(
            self.wsd, None, _MDIB_NAME, ssl_context_container=ssl_context_container, components=components
        )
        self.sdc_device.start_all()
        self.sdc_device.set_location(
            utils.random_location(),
            [pm_types.InstanceIdentifier('Validator', extension_string='System')],
        )
        consumer_components = default_components_factory()
        consumer_components.action_dispatcher_class = RequestDispatcher  # no deferred handling, easier to reason about
        self.sdc_client = SdcConsumer(
            self.sdc_device.get_xaddrs()[0],
            sdc_definitions=self.sdc_device.mdib.sdc_definitions,
            ssl_context_container=ssl_context_container,
            validate=True,
            components=consumer_components,
        )
        self.sdc_client.start_all()
        if operation_handle is None:
            operation_descriptor = get_one_descriptor_by_type(
                self.sdc_device.mdib,
                pm_types.CodedValue('0815'),
                _coded_value_comparator,
            )
            operation_handle = operation_descriptor.Handle
        future = self.sdc_client.client('Set').set_string(operation_handle=operation_handle, requested_string='ADULT')
        result = future.result(timeout=_SET_TIMEOUT)
        self.assertEqual(result.InvocationInfo.InvocationState, expected_state)
        if expected_state == msg_types.InvocationState.FINISHED:
            self.assertIsNotNone(result.InvocationSource)
        return result

    def test_known_participant_is_identified_by_pem_certificate(self):
        """By default a consumer that authenticated with a client certificate is reported by its PEM certificate."""
        result = self._invoke_set_string(use_ssl=True)
        self.assertEqual(result.InvocationSource.Root, invocationsource.X509_CERTIFICATE_PEM_ROOT)
        self.assertEqual(result.InvocationSource.Extension, _CERT_FOLDER.joinpath('test_certificate.pem').read_text())

    def test_known_participant_is_identified_by_certificate_common_name(self):
        """R0078: the provider can be configured to report the Common Name of the client certificate."""
        components = provider_components_async_factory()
        components.invocation_source_factory = invocationsource.mk_common_name_invocation_source
        result = self._invoke_set_string(use_ssl=True, components=components)
        self.assertEqual(result.InvocationSource.Root, invocationsource.DISTINGUISHED_NAME_ROOT)
        self.assertEqual(result.InvocationSource.Extension, _CERT_COMMON_NAME)

    def test_unknown_participant_is_anonymous(self):
        """R0077: a consumer without client authentication is reported as anonymous participant."""
        result = self._invoke_set_string(use_ssl=False)
        self.assertEqual(result.InvocationSource.Root, invocationsource.ANONYMOUS_PARTICIPANT_ROOT)
        self.assertEqual(result.InvocationSource.Extension, invocationsource.ANONYMOUS_PARTICIPANT_EXTENSION)

    def test_unknown_operation_does_not_identify_participant(self):
        operation_handle = 'unknown-operation'
        with mock.patch.object(SomeDevice, 'mk_invocation_source', autospec=True) as identify_participant:
            result = self._invoke_set_string(
                use_ssl=False,
                operation_handle=operation_handle,
                expected_state=msg_types.InvocationState.FAILED,
            )

        identify_participant.assert_not_called()
        self.assertEqual(result.InvocationInfo.InvocationError, msg_types.InvocationError.INVALID_VALUE)
        self.assertEqual(
            [message.text for message in result.InvocationInfo.InvocationErrorMessage],
            [f'no handler registered for "{operation_handle}"'],
        )
        self.assertGreater(result.InvocationInfo.TransactionId, 0)

    def test_identification_error_returns_failed_response_without_executing_operation(self):
        certificates: list[bytes | None] = []

        def factory(certificate: bytes | None) -> pm_types.InstanceIdentifier:
            certificates.append(certificate)
            if certificate is None:
                return invocationsource.mk_anonymous_invocation_source()
            raise ValueError('certificate identification failed')

        components = provider_components_async_factory()
        components.invocation_source_factory = factory
        with (
            mock.patch.object(SomeDevice, 'handle_operation_request', autospec=True) as handle_operation,
            self.assertLogs('sdc.device.SetService', level='ERROR') as logs,
        ):
            result = self._invoke_set_string(
                use_ssl=True,
                components=components,
                expected_state=msg_types.InvocationState.FAILED,
            )

        handle_operation.assert_not_called()
        self.assertEqual(
            certificates,
            [None, ssl.PEM_cert_to_DER_cert(_CERT_FOLDER.joinpath('test_certificate.pem').read_text())],
        )
        self.assertEqual(result.InvocationInfo.InvocationError, msg_types.InvocationError.OTHER)
        self.assertEqual(
            [message.text for message in result.InvocationInfo.InvocationErrorMessage],
            ['invoking participant could not be identified'],
        )
        self.assertGreater(result.InvocationInfo.TransactionId, 0)
        self.assertIsNone(result.InvocationSource)
        self.assertEqual(result.report_parts, [])
        self.assertEqual(result.set_response.MdibVersion, self.sdc_device.mdib.mdib_version)
        self.assertEqual(result.set_response.SequenceId, self.sdc_device.mdib.sequence_id)
        self.assertEqual(result.set_response.InstanceId, self.sdc_device.mdib.instance_id)
        self.assertIn('cannot identify invoking participant', '\n'.join(logs.output))
        self.assertIn('certificate identification failed', '\n'.join(logs.output))


class TestExecuteParametersInvocationSource(unittest.TestCase):
    """The operation handler receives the invocation source via ExecuteParameters."""

    def test_execute_operation_forwards_invocation_source_to_handler(self):
        captured: list[pm_types.InstanceIdentifier | None] = []

        def handler(params: ExecuteParameters) -> ExecuteResult:
            captured.append(params.invocation_source)
            return ExecuteResult(msg_types.InvocationState.FINISHED, mock.MagicMock())

        operation = SetStringOperation(handle='h', operation_target_handle='t', operation_handler=handler)
        invocation_source = pm_types.InstanceIdentifier('root', extension_string='CName')

        operation.execute_operation(mock.MagicMock(), mock.MagicMock(), invocation_source)

        self.assertEqual(captured, [invocation_source])

    def test_invocation_source_defaults_to_none(self):
        captured: list[pm_types.InstanceIdentifier | None] = []

        def handler(params: ExecuteParameters) -> ExecuteResult:
            captured.append(params.invocation_source)
            return ExecuteResult(msg_types.InvocationState.FINISHED, mock.MagicMock())

        operation = SetStringOperation(handle='h', operation_target_handle='t', operation_handler=handler)

        operation.execute_operation(mock.MagicMock(), mock.MagicMock())

        self.assertEqual(captured, [None])


if __name__ == '__main__':
    unittest.main()
