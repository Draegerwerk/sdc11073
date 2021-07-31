"""Compression module for pysdc."""
from collections import OrderedDict
import zlib
try:
    import lz4.frame
    _LZ4 = True
except ImportError:
    _LZ4 = False

GZIP = 'gzip'
LZ4 = 'x-lz4'
ANY = 'any'

encodings = []
if _LZ4:
    encodings.append(LZ4)
encodings.append(GZIP)

class CompressionException(Exception):
    pass

class CompressionHandler:
    """Compression handler mixin.
    Should be used by servers and clients that are supposed to handle compression
    """
    available_encodings = encodings # initial default

    @classmethod
    def compress_payload(cls, algorithm, payload):
        """Compresses payload based on required algorithm.
        Raises CompressionException if algorithm is not supported.

        @param algorithm: one of available values specified as constants in this module
        @param payload: text to compress
        @return: compressed content
        """
        if algorithm == GZIP:
            return cls._gzip_encode(payload)
        if algorithm == LZ4 and lz4 is not None:
            return lz4.frame.compress(payload)
        if _LZ4:
            txt = f"{algorithm} compression is not supported. Only gzip and lz4 are supported."
        else:
            txt = f"{algorithm} compression is not supported. Only gzip is supported."
        raise CompressionException(txt)

    @staticmethod
    def decompress(payload, algorithm):
        """Compresses payload based on required algorithm.
        Raises CompressionException if algorithm is not supported.

        @param algorithm: one of available values specified as constants in this module
        @param payload: text to decompress
        @return: decompressed content
        """
        if algorithm == GZIP:
            return zlib.decompress(payload, 16 + zlib.MAX_WBITS)
        if algorithm == LZ4 and lz4 is not None:
            return lz4.frame.decompress(payload)
        if _LZ4:
            txt = f"{algorithm} compression is not supported. Only gzip and lz4 are supported"
        else:
            txt = f"{algorithm} compression is not supported. Only gzip is supported"
        raise CompressionException(txt)

    @staticmethod
    def _gzip_encode(payload):
        gzip_compress = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
        data = gzip_compress.compress(payload) + gzip_compress.flush()
        return data

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
                parsed_headers[alg_name] = None
                try:
                    parsed_headers[alg_name] = float(alg[1].split("=")[1])
                except:
                    parsed_headers[alg_name] = 1 # default
        return [pair[0] for pair in sorted(parsed_headers.items(), key=lambda kv: kv[1], reverse=True)]
