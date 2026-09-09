"""Integration tests for msg:InvocationSource of OperationInvokedReport (IEEE 11073-20701 R0077/R0078).

Verifies end to end that an operation invoked by a consumer that authenticated with a client
certificate is reported with the certificate Common Name (R0078), while an operation invoked over an
unauthenticated connection is reported with the anonymous instance identifier (R0077).
"""

from __future__ import annotations

import logging
import pathlib
import time
import unittest

from tutorial.codedvaluecomparator import _coded_value_comparator

from sdc11073 import certloader, loghelper
from sdc11073.consumer.consumerimpl import SdcConsumer, default_components_factory
from sdc11073.dispatch import RequestDispatcher
from sdc11073.mdib import ConsumerMdib
from sdc11073.mdib.mdibaccessor import get_one_descriptor_by_type
from sdc11073.provider.porttypes import invocationsource
from sdc11073.wsdiscovery import WSDiscovery
from sdc11073.xml_types import msg_types, pm_types
from tests import utils
from tests.mockstuff import SomeDevice

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

        self.wsd = WSDiscovery('127.0.0.1')
        self.wsd.start()
        self.sdc_device_tls = SomeDevice.from_mdib_file(
            self.wsd, None, _MDIB_NAME, ssl_context_container=self.ssl_context_container
        )
        self.sdc_device_plain = SomeDevice.from_mdib_file(self.wsd, None, _MDIB_NAME)
        for device in (self.sdc_device_tls, self.sdc_device_plain):
            device.start_all()
            device.set_location(
                utils.random_location(),
                [pm_types.InstanceIdentifier('Validator', extension_string='System')],
            )
        time.sleep(0.5)

        self.sdc_client = None
        self._logger.info('############### setUp done %s ##############', self._testMethodName)

    def tearDown(self):
        self._logger.info('############### tearDown %s ... ##############', self._testMethodName)
        if self.sdc_client is not None:
            self.sdc_client.stop_all()
        self.sdc_device_tls.stop_all()
        self.sdc_device_plain.stop_all()
        self.wsd.stop()

    def _connect_consumer(self, device: SomeDevice, *, use_ssl: bool) -> SdcConsumer:
        consumer_components = default_components_factory()
        consumer_components.action_dispatcher_class = RequestDispatcher  # no deferred handling, easier to reason about
        consumer = SdcConsumer(
            device.get_xaddrs()[0],
            sdc_definitions=device.mdib.sdc_definitions,
            ssl_context_container=self.ssl_context_container if use_ssl else None,
            validate=True,
            components=consumer_components,
        )
        consumer.start_all()
        self.sdc_client = consumer
        time.sleep(1)
        return consumer

    def _invoke_set_string(self, device: SomeDevice, consumer: SdcConsumer) -> msg_types.OperationInvokedReportPart:
        consumer_mdib = ConsumerMdib(consumer)
        consumer_mdib.init_mdib()
        operation_descriptor = get_one_descriptor_by_type(
            device.mdib,
            pm_types.CodedValue('0815'),
            _coded_value_comparator,
        )
        future = consumer.client('Set').set_string(
            operation_handle=operation_descriptor.Handle, requested_string='ADULT'
        )
        result = future.result(timeout=_SET_TIMEOUT)
        self.assertEqual(result.InvocationInfo.InvocationState, msg_types.InvocationState.FINISHED)
        return result

    def test_known_participant_is_identified_by_certificate_common_name(self):
        """R0078: a consumer that authenticated with a client certificate is reported by its Common Name."""
        consumer = self._connect_consumer(self.sdc_device_tls, use_ssl=True)
        result = self._invoke_set_string(self.sdc_device_tls, consumer)
        self.assertIsNotNone(result.InvocationSource)
        self.assertEqual(result.InvocationSource.Root, invocationsource.DISTINGUISHED_NAME_ROOT)
        self.assertEqual(result.InvocationSource.Extension, _CERT_COMMON_NAME)

    def test_unknown_participant_is_anonymous(self):
        """R0077: a consumer without client authentication is reported as anonymous participant."""
        consumer = self._connect_consumer(self.sdc_device_plain, use_ssl=False)
        result = self._invoke_set_string(self.sdc_device_plain, consumer)
        self.assertIsNotNone(result.InvocationSource)
        self.assertEqual(result.InvocationSource.Root, invocationsource.ANONYMOUS_PARTICIPANT_ROOT)
        self.assertEqual(result.InvocationSource.Extension, invocationsource.ANONYMOUS_PARTICIPANT_EXTENSION)


if __name__ == '__main__':
    unittest.main()
