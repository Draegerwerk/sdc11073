"""Unit tests for provider HTTP server timeout error handling."""

import pathlib
import unittest
from typing import Any
from unittest import mock

from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.mdib import ProviderMdib
from sdc11073.provider.providerimpl import SdcProvider

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
