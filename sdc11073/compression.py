"""Compression module for http. """
import zlib
from collections import OrderedDict

try:
    import lz4.frame

    _LZ4 = True
except ImportError:
    _LZ4 = False

class CompressionException(Exception):
    pass


class CompressionHandler:
    """Compression handler.
    Should be used by servers and clients that are supposed to handle compression
    """
    available_encodings = []  # encodings # initial default
    handlers = {}

    @classmethod
    def register_handler(cls, handler):
        for alg in handler.algorithms:
            cls.available_encodings.append(alg)
            cls.handlers[alg] = handler

    @classmethod
    def compress_payload(cls, algorithm, payload):
        """Compresses payload based on required algorithm.
        Raises CompressionException if algorithm is not supported.

        @param algorithm: one of available values specified as constants in this module
        @param payload: text to compress
        @return: compressed content
        """
        return cls.get_handler(algorithm).compress_payload(payload)

    @classmethod
    def decompress_payload(cls, algorithm, payload):
        """Compresses payload based on required algorithm.
        Raises CompressionException if algorithm is not supported.

        @param algorithm: one of available values specified as constants in this module
        @param payload: text to decompress
        @return: decompressed content
        """
        return cls.get_handler(algorithm).decompress_payload(payload)

    @classmethod
    def get_handler(cls, algorithm):
        handler = cls.handlers.get(algorithm)
        if not handler:
            txt = f"{algorithm} compression is not supported. Only {cls.available_encodings} are supported."
            raise CompressionException(txt)
        return handler

    @staticmethod
    def parse_header(header):
        """
        Examples of headers are:  Examples of its use are:

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
                try:
                    parsed_headers[alg_name] = float(alg[1].split("=")[1])
                except (ValueError, IndexError):
                    pass
        return [pair[0] for pair in sorted(parsed_headers.items(), key=lambda kv: kv[1], reverse=True)]


class GzipCompressionHandler:
    algorithms = ('gzip',)

    @staticmethod
    def compress_payload(payload):
        gzip_compress = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
        data = gzip_compress.compress(payload) + gzip_compress.flush()
        return data

    @staticmethod
    def decompress_payload(payload):
        return zlib.decompress(payload, 16 + zlib.MAX_WBITS)


CompressionHandler.register_handler(GzipCompressionHandler)


class Lz4CompressionHandler:
    algorithms = ('x-lz4', 'lz4')

    @staticmethod
    def compress_payload(payload):
        return lz4.frame.compress(payload)

    @staticmethod
    def decompress_payload(payload):
        return lz4.frame.decompress(payload)


if _LZ4:
    CompressionHandler.register_handler(Lz4CompressionHandler)
