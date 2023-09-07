from __future__ import annotations

from io import BytesIO

from .compression import CompressionHandler

""" This module handles reading http messages. It supports chunking and de-compression"""

class DechunkError(Exception):
    """Raised when could not de-chunk stream.
    """


class DecompressError(Exception):
    """Raised when could not de-compress stream.
    """


def mk_chunks(body, chunk_size=512):
    """
    convert plain body bytes to chunked bytes
    :param body: bytes
    :param chunk_size: size of chunks
    :return: body converted to chunks (but still as single bytes array)
    """
    data = BytesIO()
    tail = body
    while True:
        head, tail = tail[:chunk_size], tail[chunk_size:]
        data.write(f'{len(head):x}\r\n'.encode())  # defaults to utf-8 encoding
        data.write(head)
        data.write(b'\r\n')
        if not head:
            return data.getvalue()


CR_LF = b'\r\n'


class HTTPReader:
    """ Base class that implements decoding of incoming http requests.
    Supported features:
    - read data by content-length
    - handle chunk-encoding
    - handle compression
    """

    @classmethod
    def _read_dechunk(cls, stream):
        """De-chunk HTTP body stream.
        :param file stream: readable file-like object.
        :rtype: bytes
        :raise: DechunkError
        """
        body = []
        while True:
            chunk_header = cls._read_until(stream, CR_LF)
            chunk_headers = chunk_header.split(b';')  # length + optional chunk-extensions (name=value pairs)
            chunk_len, _ = chunk_headers[0], chunk_headers[1:]  # we do nothing with chunk-extensions...
            if chunk_len is None:
                raise DechunkError(
                    'Could not extract chunk size: unexpected end of data.')

            try:
                chunk_len = int(chunk_len.strip(), 16)
            except (ValueError, TypeError) as err:
                raise DechunkError('Could not parse chunk size:') from err

            bytes_to_read = chunk_len
            while bytes_to_read:
                chunk = stream.read(bytes_to_read)
                bytes_to_read -= len(chunk)
                body.append(chunk)

            # chunk ends with \r\n
            cr_lf = stream.read(2)
            if cr_lf != CR_LF:
                raise DechunkError('No CR+LF at the end of chunk!')
            if chunk_len == 0:  # len == 0 indicates end of data
                break
        return b''.join(body)

    @staticmethod
    def _read_until(stream, delimiter, max_bytes=16):
        """Read until we have found the given delimiter.
        :param file stream: readable file-like object.
        :param bytes delimiter: delimiter string.
        :param int max_bytes: maximum bytes to read.
        :rtype: bytes|None
        """
        buf = bytearray()
        delim_len = len(delimiter)

        while len(buf) < max_bytes:
            char = stream.read(1)

            if not char:
                break

            buf += char
            if buf[-delim_len:] == delimiter:
                return bytes(buf[:-delim_len])
        return None

    @classmethod
    def read_request_body(cls, http_message, supported_encodings=None):
        """ checks header for content-length, chunk-encoding and compression entries.
        Handles incoming bytes correspondingly.
        @http_message: a http request or response read from network
        :return: bytes
        """
        http_body = None
        transfer_encoding = http_message.headers.get('transfer-encoding')
        if transfer_encoding is not None and transfer_encoding.lower() == 'chunked':
            http_body = cls._read_dechunk(http_message.rfile)
        else:
            cl_string = http_message.headers.get('content-length')
            if cl_string:
                try:
                    content_length = int(cl_string)
                    http_body = http_message.rfile.read(content_length)
                except TypeError:
                    http_body = http_message.rfile.read()

        # if we get compressed content then we check against server setting
        # if it matches continue and decompress
        # if current server setting is any, use whatever client has provided in content-encoding header
        actual_enc = http_message.headers.get('content-encoding')
        if actual_enc:
            supported_encs = supported_encodings or CompressionHandler.available_encodings
            if actual_enc in supported_encs:
                http_body = CompressionHandler.decompress_payload(actual_enc, http_body)
            else:
                raise DecompressError(f'content-encoding "{actual_enc}" is not supported', )
        return http_body

    @classmethod
    def read_response_body(cls, http_response, supported_encodings=None):
        """ checks header for content-length, chunk-encoding and compression entries.
        Handles incoming bytes correspondingly.
        :http_response: a http response read from network
        :supported_encodings: if given, only these encodings may be used.
        :return: bytes
        """
        http_body = None
        cl_string = http_response.getheader('content-length')
        if cl_string:
            try:
                content_length = int(cl_string)
                http_body = http_response.read(content_length)
            except TypeError:
                http_body = http_response.read()
        if http_body is None:
            transfer_encoding = http_response.getheader('transfer-encoding')
            if transfer_encoding is not None and transfer_encoding.lower() == 'chunked':
                # de-chunking is done by http client. we just need to read until no more data available
                http_body = []
                tmp = http_response.read()
                while tmp:
                    http_body.append(tmp)
                    tmp = http_response.read()
                http_body = b''.join(http_body)
        if http_body is None:
            http_body = http_response.read()

        # if we get compressed content then we check against server setting
        # if it matches continue and decompress
        # if current server setting is any, use whatever client has provided in content-encoding header
        actual_enc = http_response.getheader('content-encoding')
        if actual_enc:
            supported_encs = supported_encodings or CompressionHandler.available_encodings
            if actual_enc in supported_encs:
                http_body = CompressionHandler.decompress_payload(actual_enc, http_body)
            else:
                raise DecompressError(f'content-encoding "{actual_enc}" is not supported')
        return http_body
