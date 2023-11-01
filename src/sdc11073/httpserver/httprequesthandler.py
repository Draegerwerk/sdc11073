from __future__ import annotations

from http.server import BaseHTTPRequestHandler
from urllib.parse import urlparse

from .compression import CompressionHandler
from .httpreader import HTTPReader, mk_chunks
from sdc11073.exceptions import InvalidPathError


class DispatchingRequestHandler(BaseHTTPRequestHandler):
    """This request handler expects that the http server has a 'dispatcher' member that is a PathElementRegistry.
     The registered Components must be a MessageConverterMiddleware. """
    protocol_version = "HTTP/1.1"  # this enables keep-alive

    # This server does NOT disable nagle algorithm. It sends Large responses,
    # and network efficiency is more important than short latencies.
    disable_nagle_algorithm = False

    def _read_request(self):
        """ checks header for content-length, chunk-encoding and compression entries.
        Handles incoming bytes correspondingly.
        :return: http body as bytes
        """
        return HTTPReader.read_request_body(self)

    def _compress_if_supported(self, response_bytes: bytes):
        """Compress response if header of request indicates that other side
        accepts one of our supported compression encodings"""
        accepted_enc = CompressionHandler.parse_header(self.headers.get('accept-encoding'))
        for enc in accepted_enc:
            if enc in self.server.supported_encodings:
                response_bytes = CompressionHandler.compress_payload(enc, response_bytes)
                self.send_header('Content-Encoding', enc)
                break
        return response_bytes

    def log_request(self, code='-', size='-'):
        pass  # suppress printing of every request to stderr

    def get_first_path_element(self):
        parsed_path = urlparse(self.path)
        path_elements = parsed_path.path.split('/')
        if len(path_elements[0]) > 0:
            return path_elements[0]
        return path_elements[1]

    def do_POST(self):  # pylint: disable=invalid-name
        request_bytes = self._read_request()
        if self.server.dispatcher is None:
            # close this connection
            self.close_connection = True  # pylint: disable=attribute-defined-outside-init
            http_reason = 'received a POST request, but have no dispatcher'
            response_xml_string = http_reason.encode('utf-8')
            self.send_response(500, http_reason)  # server error
            self.send_header("Content-type", "text/plain; charset=utf-8")
            self.send_header("Content-length", str(len(response_xml_string)))
            self.end_headers()
            self.wfile.write(response_xml_string)
            return
        try:
            component = self.server.dispatcher.get_instance(self.get_first_path_element())
        except InvalidPathError as ex:
            self.server.logger.error('invalid path {} (request from {}): {}', self.path, self.client_address, ex.reason)
            http_reason = ex.reason
            response_xml_string = b''
            self.send_response(ex.status, http_reason)
            self.send_header("Content-type", "text/plain; charset=utf-8")
            self.send_header("Content-length", str(len(response_xml_string)))
            self.end_headers()
            self.wfile.write(response_xml_string)
            return

        peer_name = self.connection.getpeername()
        try:
            result = component.do_post(self.headers, self.path, peer_name, request_bytes)
            http_status, http_reason, response_xml_string = result
        except Exception as ex:
            self.server.logger.error('exception (request from {}): {}', self.path, self.client_address, ex)
            http_reason = str(ex)
            response_xml_string = b''
            self.send_response(500, http_reason)  # server error
            self.send_header("Content-type", "text/plain; charset=utf-8")
            self.send_header("Content-length", str(len(response_xml_string)))
            self.end_headers()
            self.wfile.write(response_xml_string)
            return

        self.send_response(http_status, http_reason)
        response_xml_string = self._compress_if_supported(response_xml_string)
        self.send_header("Content-type", "application/soap+xml; charset=utf-8")
        if self.server.chunk_size > 0:
            self.send_header("transfer-encoding", "chunked")
            self.end_headers()
            self.wfile.write(mk_chunks(response_xml_string, chunk_size=self.server.chunk_size))
        else:
            self.send_header("Content-length", str(len(response_xml_string)))
            self.end_headers()
            self.wfile.write(response_xml_string)

    def do_GET(self):  # pylint: disable=invalid-name
        if self.server.dispatcher is None:
            # close this connection
            self.close_connection = True  # pylint: disable=attribute-defined-outside-init
            response_xml_string = 'received a POST request, but have no dispatcher'
            self.send_response(404, response_xml_string)  # not found
            return

        component = self.server.dispatcher.get_instance(self.get_first_path_element())

        peer_name = self.connection.getpeername()
        result = component.do_get(self.headers, self.path, peer_name)
        http_status, http_reason, response_xml_string, content_type = result

        self.send_response(http_status, http_reason)
        response_xml_string = self._compress_if_supported(response_xml_string)
        self.send_header("Content-type", content_type)
        if self.server.chunk_size > 0:
            self.send_header("transfer-encoding", "chunked")
            self.end_headers()
            self.wfile.write(mk_chunks(response_xml_string, chunk_size=self.server.chunk_size))
        else:
            self.send_header("Content-length", str(len(response_xml_string)))
            self.end_headers()
            self.wfile.write(response_xml_string)
