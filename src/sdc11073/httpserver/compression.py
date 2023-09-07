"""Compression module for http."""
import contextlib
import zlib
from abc import ABC, abstractmethod
from collections import OrderedDict
from typing import ClassVar

try:
    import lz4.frame
except ImportError:
    lz4 = None


class CompressionError(Exception):
    pass


class AbstractDataCompressor(ABC):
    algorithms = ()

    @staticmethod
    @abstractmethod
    def compress_payload(payload):
        pass

    @staticmethod
    @abstractmethod
    def decompress_payload(payload):
        pass


class CompressionHandler:
    """Compression handler.
    Should be used by servers and clients that are supposed to handle compression.
    """

    available_encodings: ClassVar[list[str]] = []  # initial default
    handlers: ClassVar[dict[str, type[AbstractDataCompressor]]] = {}

    @classmethod
    def register_handler(cls, handler: type[AbstractDataCompressor]):
        for alg in handler.algorithms:
            if alg.lower() in cls.available_encodings:
                raise ValueError(f'Algorithm {alg} already registered, class = {cls.__name__} ')
        for alg in handler.algorithms:
            cls.available_encodings.append(alg.lower())
            cls.handlers[alg] = handler

    @classmethod
    def compress_payload(cls, algorithm: str, payload: bytes):
        """Compresses payload based on required algorithm.
        Raises CompressionException if algorithm is not supported.

        :param algorithm: one of strings provided by registered compression handlers
        :param payload: text to compress
        @return: compressed content
        """
        return cls.get_handler(algorithm).compress_payload(payload)

    @classmethod
    def decompress_payload(cls, algorithm: str, payload: bytes):
        """Decompresses payload based on required algorithm.
        Raises CompressionException if algorithm is not supported.

        :param algorithm: one of strings provided by registered compression handlers
        :param payload: text to decompress
        @return: decompressed content
        """
        return cls.get_handler(algorithm).decompress_payload(payload)

    @classmethod
    def get_handler(cls, algorithm: str):
        """:param algorithm: one of strings provided by registered compression handlers
        :return: AbstractDataCompressor implementation
        """
        handler = cls.handlers.get(algorithm.lower())
        if not handler:
            txt = f"{algorithm} compression is not supported. Only {cls.available_encodings} are supported."
            raise CompressionError(txt)
        return handler

    @staticmethod
    def parse_header(header):
        """Examples of headers are:  Examples of its use are:

        Accept-Encoding: compress, gzip
        Accept-Encoding:
        Accept-Encoding: *
        Accept-Encoding: compress;q=0.5, gzip;q=1.0
        Accept-Encoding: gzip;q=1.0, identity; q=0.5, *;q=0

        returns sorted list of compression algorithms by priority
        """
        # for now work with standard python containers
        # if performance becomes an issue could be done within one loop
        parsed_headers = OrderedDict()
        if header:
            for alg in (x.split(";") for x in header.split(",")):
                alg_name = alg[0].strip()
                parsed_headers[alg_name] = 1  # default
                with contextlib.suppress(ValueError, IndexError):
                    parsed_headers[alg_name] = float(alg[1].split("=")[1])

        return [pair[0] for pair in sorted(parsed_headers.items(), key=lambda kv: kv[1], reverse=True)]


class GzipCompressionHandler(AbstractDataCompressor):
    algorithms = ('gzip',)

    @staticmethod
    def compress_payload(payload: bytes):
        if not isinstance(payload, bytes):
            raise TypeError(f'a bytes-like object is required, not "{payload.__class__.__name__}", payload={payload}')
        gzip_compress = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
        return gzip_compress.compress(payload) + gzip_compress.flush()

    @staticmethod
    def decompress_payload(payload: bytes):
        return zlib.decompress(payload, 16 + zlib.MAX_WBITS)


CompressionHandler.register_handler(GzipCompressionHandler)


class Lz4CompressionHandler(AbstractDataCompressor):
    algorithms = ('x-lz4', 'lz4')

    @staticmethod
    def compress_payload(payload: bytes):
        return lz4.frame.compress(payload)

    @staticmethod
    def decompress_payload(payload: bytes):
        return lz4.frame.decompress(payload)


if lz4 is not None:
    CompressionHandler.register_handler(Lz4CompressionHandler)
