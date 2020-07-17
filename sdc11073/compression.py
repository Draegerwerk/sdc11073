"""Compression module for pysdc."""
from collections import OrderedDict
import zlib
try:
    import lz4.frame
except ImportError:
    lz4 = None

GZIP = 'gzip'
LZ4 = 'x-lz4'
ANY = 'any'

encodings = []
if lz4 is not None:
    encodings.append(LZ4)
encodings.append(GZIP)

class CompressionException(Exception):
    pass

class CompressionHandler(object):
    """Compression handler mixin.
    Should be used by servers and clients that are supposed to handle compression
    """
    available_encodings = encodings # initial default

    @classmethod
    def compressPayload(cls, algorithm, payload):
        """Compresses payload based on required algorithm.
        Raises CompressionException if algorithm is not supported.

        @param algorithm: one of available values specified as constants in this module
        @param payload: text to compress
        @return: compressed content
        """
        if algorithm == GZIP:
            return cls._gzip_encode(payload)
        elif algorithm == LZ4 and lz4 is not None:
            return lz4.frame.compress(payload)
        else:
            if lz4 is not None:
                raise CompressionException("{} compression is not supported. "
                                           "Only gzip and lz4 are supported".format(algorithm))
            else:
                raise CompressionException("{} compression is not supported. "
                                           "Only gzip is supported".format(algorithm))

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
        elif algorithm == LZ4 and lz4 is not None:
            return lz4.frame.decompress(payload)
        else:
            if lz4 is not None:
                raise CompressionException("{} compression is not supported. "
                                           "Only gzip and lz4 are supported".format(algorithm))
            else:
                raise CompressionException("{} compression is not supported. "
                                           "Only gzip is supported".format(algorithm))

    @staticmethod
    def _gzip_encode(payload):
        gzip_compress = zlib.compressobj(zlib.Z_DEFAULT_COMPRESSION, zlib.DEFLATED, 16 + zlib.MAX_WBITS)
        data = gzip_compress.compress(payload) + gzip_compress.flush()
        return data

    @staticmethod
    def parseHeader(header):
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
        parsedHeaders = OrderedDict()
        if header:
            for alg in (x.split(";") for x in header.split(",")):
                algName = alg[0].strip()
                parsedHeaders[algName] = None
                try:
                    parsedHeaders[algName] = float(alg[1].split("=")[1])
                except:
                    parsedHeaders[algName] = 1 # default
        return [pair[0] for pair in sorted(parsedHeaders.items(), key=lambda kv: kv[1], reverse=True)]
