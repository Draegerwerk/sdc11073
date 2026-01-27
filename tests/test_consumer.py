"""Tests for SDC Consumer functionality."""

import unittest
from unittest import mock

from sdc11073.consumer import SdcConsumer
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.httpserver.httpserverimpl import HttpServerThreadBase


class TestConsumerWithFailure(unittest.TestCase):
    def test_fail_on_startup(self):
        """Test that starting services fails with RuntimeError if HTTP server does not start in time."""
        with mock.patch.object(HttpServerThreadBase, 'start'):
            sdc_consumer = SdcConsumer('http', SdcV1Definitions, None)
            sdc_consumer._network_adapter = mock.Mock()
            sdc_consumer._network_adapter.ip = '123.456.789.000'

            with self.assertRaises(RuntimeError) as context:
                sdc_consumer._start_event_sink(shared_http_server=None, http_server_timeout=0)
            self.assertIn('Http server could not be started', str(context.exception))
