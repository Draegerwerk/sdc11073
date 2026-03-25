"""Tests for MessageConverterMiddleware."""

from __future__ import annotations

import logging
import uuid
from unittest.mock import MagicMock

import pytest

from sdc11073.dispatch.messageconverter import MessageConverterMiddleware
from sdc11073.exceptions import HTTPRequestHandlingError
from sdc11073.pysoap.soapenvelope import Fault, faultcodeEnum


@pytest.fixture
def mock_msg_reader() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_msg_factory() -> MagicMock:
    return MagicMock()


@pytest.fixture
def mock_dispatcher() -> MagicMock:
    return MagicMock()


@pytest.fixture
def middleware(
    mock_msg_reader: MagicMock, mock_msg_factory: MagicMock, mock_dispatcher: MagicMock
) -> MessageConverterMiddleware:
    return MessageConverterMiddleware(mock_msg_reader, mock_msg_factory, MagicMock(), mock_dispatcher)


class TestDoPost:
    """Tests for do_post method."""

    def test_successful_post(
        self, middleware: MessageConverterMiddleware, mock_msg_reader: MagicMock, mock_dispatcher: MagicMock
    ):
        """Test successful POST dispatching."""
        mock_msg_reader.read_received_message.return_value = MagicMock()
        mock_response = MagicMock()
        mock_response.serialize.return_value = uuid.uuid4()
        mock_dispatcher.on_post.return_value = mock_response

        status, reason, body = middleware.do_post({}, '/uuid/path', '127.0.0.1', b'<request/>')

        assert status == 200
        assert reason == 'Ok'
        assert body == mock_response.serialize.return_value
        mock_msg_reader.read_received_message.assert_called_once_with(b'<request/>')
        mock_dispatcher.on_post.assert_called_once()

    def test_read_raises_http_request_handling_error(
        self, mock_msg_reader: MagicMock, mock_msg_factory: MagicMock, mock_dispatcher: MagicMock
    ):
        """Test fault response when msg_reader raises HTTPRequestHandlingError."""
        mw = MessageConverterMiddleware(mock_msg_reader, mock_msg_factory, MagicMock(), mock_dispatcher)
        # Create a real Fault to avoid mock issues with Code.Value assignment
        soap_fault = Fault()
        soap_fault.Code.Value = faultcodeEnum.SENDER
        soap_fault.add_reason_text('bad request')
        mock_msg_reader.read_received_message.side_effect = HTTPRequestHandlingError(400, 'Bad Request', soap_fault)
        mock_response = MagicMock()
        mock_response.serialize.return_value = uuid.uuid4()
        mock_msg_factory.mk_soap_message.return_value = mock_response

        status, reason, body = mw.do_post({}, '/uuid/path', '127.0.0.1', b'<bad/>')

        assert status == 400
        assert reason == 'Bad Request'
        assert body == mock_response.serialize.return_value
        mock_msg_factory.mk_soap_message.assert_called_once()

    def test_read_raises_generic_exception(
        self, middleware: MessageConverterMiddleware, mock_msg_reader: MagicMock, mock_msg_factory: MagicMock
    ):
        """Test fault response when msg_reader raises a generic Exception."""
        mock_msg_reader.read_received_message.side_effect = ValueError('parse error')
        mock_response = MagicMock()
        mock_response.serialize.return_value = uuid.uuid4()
        mock_msg_factory.mk_soap_message.return_value = mock_response

        status, reason, body = middleware.do_post({}, '/uuid/path', '127.0.0.1', b'<bad/>')

        assert status == 500
        assert reason == 'exception'
        assert body == mock_response.serialize.return_value
        mock_msg_factory.mk_soap_message.assert_called_once()

    def test_dispatch_raises_http_request_handling_error(
        self,
        middleware: MessageConverterMiddleware,
        mock_msg_reader: MagicMock,
        mock_msg_factory: MagicMock,
        mock_dispatcher: MagicMock,
    ):
        """Test fault when dispatcher.on_post raises HTTPRequestHandlingError."""
        mock_msg_reader.read_received_message.return_value = MagicMock()
        soap_fault = MagicMock()
        mock_dispatcher.on_post.side_effect = HTTPRequestHandlingError(404, 'Not Found', soap_fault)
        mock_reply = MagicMock()
        mock_reply.serialize.return_value = uuid.uuid4()
        mock_msg_factory.mk_reply_soap_message.return_value = mock_reply

        status, reason, body = middleware.do_post({}, '/uuid/path', '127.0.0.1', b'<request/>')

        assert status == 404
        assert reason == 'Not Found'
        assert body == mock_reply.serialize.return_value
        mock_msg_factory.mk_reply_soap_message.assert_called_once()
        # read_received_message called twice: once for initial read, once for re-read without validation
        assert mock_msg_reader.read_received_message.call_count == 2
        mock_msg_reader.read_received_message.assert_any_call(b'<request/>', validate=False)

    def test_dispatch_raises_generic_exception(
        self,
        middleware: MessageConverterMiddleware,
        mock_msg_reader: MagicMock,
        mock_msg_factory: MagicMock,
        mock_dispatcher: MagicMock,
    ):
        """Test 500 response when dispatcher.on_post raises a generic Exception."""
        mock_msg_reader.read_received_message.return_value = MagicMock()
        mock_dispatcher.on_post.side_effect = RuntimeError('unexpected error')
        mock_reply = MagicMock()
        mock_reply.serialize.return_value = uuid.uuid4()
        mock_msg_factory.mk_reply_soap_message.return_value = mock_reply

        status, reason, body = middleware.do_post({}, '/uuid/path', '127.0.0.1', b'<request/>')

        assert status == 500
        assert reason == 'exception'
        assert body == mock_reply.serialize.return_value
        mock_msg_factory.mk_reply_soap_message.assert_called_once()
        # The fault passed to mk_reply_soap_message should be a Fault
        call_args = mock_msg_factory.mk_reply_soap_message.call_args
        fault_arg = call_args[0][1]
        assert isinstance(fault_arg, Fault)

    def test_post_consumes_path_element(
        self, middleware: MessageConverterMiddleware, mock_msg_reader: MagicMock, mock_dispatcher: MagicMock
    ):
        """Test that do_post consumes the first path element (uuid)."""
        mock_msg_reader.read_received_message.return_value = MagicMock()
        mock_response = MagicMock()
        mock_response.serialize.return_value = '<response/>'
        mock_dispatcher.on_post.return_value = mock_response

        middleware.do_post({}, '/my-uuid/service', '127.0.0.1', b'<request/>')

        # Verify the request_data passed to dispatcher had consume_current_path_element called
        call_args = mock_dispatcher.on_post.call_args
        request_data = call_args[0][0]
        assert 'my-uuid' in request_data.consumed_path_elements


class TestDoGet:
    """Tests for do_get method."""

    def test_successful_get_soap(self, middleware: MessageConverterMiddleware, mock_dispatcher: MagicMock):
        """Test successful GET returning soap+xml content type."""
        mock_dispatcher.on_get.return_value = uuid.uuid4()

        status, reason, body, content_type = middleware.do_get({}, '/uuid/path', '127.0.0.1')

        assert status == 200
        assert reason == 'Ok'
        assert body == mock_dispatcher.on_get.return_value
        assert content_type == 'application/soap+xml; charset=utf-8'

    def test_successful_get_wsdl(self, middleware: MessageConverterMiddleware, mock_dispatcher: MagicMock):
        """Test successful GET for WSDL returns text/xml content type."""
        mock_dispatcher.on_get.return_value = uuid.uuid4()

        status, reason, body, content_type = middleware.do_get({}, '/uuid/path?wsdl', '127.0.0.1')

        assert status == 200
        assert reason == 'Ok'
        assert body == mock_dispatcher.on_get.return_value
        assert content_type == 'text/xml; charset=utf-8'

    def test_get_raises_exception(self, middleware: MessageConverterMiddleware, mock_dispatcher: MagicMock):
        """Test 500 response when dispatcher.on_get raises an Exception."""
        exception_msg = uuid.uuid4().hex
        mock_dispatcher.on_get.side_effect = RuntimeError(exception_msg)

        status, reason, body, content_type = middleware.do_get({}, '/uuid/path', '127.0.0.1')

        assert status == 500
        assert reason == 'Exception'
        assert body == exception_msg.encode('utf-8')
        assert content_type == 'text'

    def test_get_consumes_path_element(self, middleware: MessageConverterMiddleware, mock_dispatcher: MagicMock):
        """Test that do_get consumes the first path element (uuid)."""
        mock_dispatcher.on_get.return_value = '<response/>'

        middleware.do_get({}, '/my-uuid/service', '127.0.0.1')

        call_args = mock_dispatcher.on_get.call_args
        request_data = call_args[0][0]
        assert 'my-uuid' in request_data.consumed_path_elements
