from http.client import HTTPException, NotConnected
from unittest import TestCase, mock

from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.loghelper import get_logger_adapter
from sdc11073.pysoap.msgfactory import MessageFactory
from sdc11073.pysoap.msgreader import MessageReader, ReceivedMessage
from sdc11073.pysoap.soapclient import HTTPReturnCodeError, SoapClient
from sdc11073.xml_types.addressing_types import HeaderInformationBlock
from sdc11073.xml_types.eventing_types import Renew, RenewResponse


class TestSoapClient(TestCase):
    """Test error handling of SoapClient."""

    def setUp(self):
        """Create the soap client used in the test."""
        self.logger = get_logger_adapter('test')

        self.soap_client = SoapClient(netloc='127.0.0.1:9999',
                                      socket_timeout=10,
                                      logger=self.logger,
                                      ssl_context=None,
                                      sdc_definitions=SdcV1Definitions,
                                      msg_reader=MessageReader(SdcV1Definitions, None, self.logger, validate=False))

    def test_get_from_url(self):
        """Check error handling of get_from_url method."""
        body = b''  # body content relevant in this test
        self._setup_mock(body)

        # normal call, no exception
        self.soap_client.get_from_url('foo', msg='')

        # http connection raises an exception => exception is delegated to caller
        for exception in (HTTPException('mock'), OSError('mock'), Exception('mock')):
            mocked_connection, http_response = self._setup_mock(body)

            mocked_connection.getresponse.side_effect = exception
            self.assertRaises(exception.__class__, self.soap_client.get_from_url, 'foo', '')
            self.assertFalse(self.soap_client.is_closed())

    def test_post_message_to(self):
        """Check error handling of post_message_to method."""
        # create a message to be sent
        factory = MessageFactory(SdcV1Definitions, None, self.logger)
        inf = HeaderInformationBlock(action='some_action', addr_to='does_not_matter')
        payload = Renew()
        created_message = factory.mk_soap_message(inf, payload)

        # create a message to be received
        factory = MessageFactory(SdcV1Definitions, None, get_logger_adapter('mock'))
        payload = RenewResponse()
        payload.Expires = 10
        inf = HeaderInformationBlock(action='some_action', addr_to='does_not_matter')
        returned_message = factory.mk_soap_message(inf, payload)
        body = returned_message.serialize()

        # mock the connection
        self._setup_mock(body)

        # normal call, no exception
        result = self.soap_client.post_message_to('renew', created_message, validate=False)
        self.assertIsInstance(result, ReceivedMessage)
        self.assertFalse(self.soap_client.is_closed())

        # request method of http connection raises an Exception => exception is converted to NotConnected
        for exception in (HTTPException('mock'), OSError('mock'), Exception('mock')):
            mocked_connection, http_response = self._setup_mock(body)
            mocked_connection.request.side_effect = exception
            self.assertRaises(NotConnected, self.soap_client.post_message_to, 'renew', created_message, validate=False)
            self.assertTrue(self.soap_client.is_closed())

        # getresponse method of http connection raises an Exception => exception is converted to NotConnected
        for exception in (HTTPException('mock'), OSError('mock'), Exception('mock')):
            mocked_connection, http_response = self._setup_mock(body)
            mocked_connection.getresponse.side_effect = exception
            self.assertRaises(NotConnected, self.soap_client.post_message_to, 'renew', created_message, validate=False)
            self.assertTrue(self.soap_client.is_closed())

        # returned status >= 300 => raise HTTPReturnCodeError, connection stays open
        mocked_connection, http_response = self._setup_mock(body)
        http_response.status = 333
        self.assertRaises(HTTPReturnCodeError, self.soap_client.post_message_to, 'renew', created_message,
                          validate=False)
        self.assertFalse(self.soap_client.is_closed())

    def _setup_mock(self, body: bytes) -> tuple[mock.MagicMock, mock.MagicMock]:
        """Mock self.soap_client._http_connection.

        The mock returns a mocked HttpResponse with body as data on getresponse() call.
        Return the mocked connection and the mocked http response, so that they can be manipulated in test.
        """
        mocked_connection = mock.MagicMock()
        http_response = mock.MagicMock()
        http_response.status = 200
        http_response.getheader.side_effect = [f'{len(body)}', None]  # content length, transfer-encoding
        http_response.read.return_value = body
        mocked_connection.getresponse.return_value = http_response
        self.soap_client._http_connection = mocked_connection  # noqa: SLF001
        self.soap_client._has_connection_error = False # noqa: SLF001
        return mocked_connection, http_response
