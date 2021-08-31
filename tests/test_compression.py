import unittest
import time
import sdc11073
from tests.mockstuff import SomeDevice
from sdc11073.sdcclient import SdcClient
from lxml import etree
from sdc11073 import compression

XML_REQ = '<?xml version=\'1.0\' encoding=\'UTF-8\'?> \
<s12:Envelope xmlns:dom="__BICEPS_ParticipantModel__" xmlns:dpws="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01"' \
          ' xmlns:ext="__ExtensionPoint__" xmlns:msg="__BICEPS_MessageModel__" xmlns:s12="http://www.w3.org/2003/05/soap-envelope"' \
          ' xmlns:si="http://standards.ieee.org/downloads/11073/11073-20702-2016/" xmlns:wsa="http://www.w3.org/2005/08/addressing"' \
          ' xmlns:wsd="http://docs.oasis-open.org/ws-dd/ns/discovery/2009/01" xmlns:wse="http://schemas.xmlsoap.org/ws/2004/08/eventing"' \
          ' xmlns:wsx="http://schemas.xmlsoap.org/ws/2004/09/mex" xmlns:xsd="http://www.w3.org/2001/XMLSchema"' \
          ' xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"><s12:Header>' \
          '<wsa:To s12:mustUnderstand="true">https://127.0.0.1:60373/53b05eb06edf11e8bc9a00059a3c7a00/Set</wsa:To>' \
          '<wsa:Action s12:mustUnderstand="true">http://schemas.xmlsoap.org/ws/2004/09/mex/GetMetadata/Request</wsa:Action>' \
          '<wsa:MessageID>urn:uuid:5837db9c-63a0-4f5c-99a3-9fc40ae61ba6</wsa:MessageID></s12:Header><s12:Body><wsx:GetMetadata/>' \
          '</s12:Body></s12:Envelope>'

GZIP = compression.GzipCompressionHandler.algorithms[0]
LZ4 = compression.Lz4CompressionHandler.algorithms[0]

class Test_Compression(unittest.TestCase):

    def setUp(self):
        # Start discovery
        self.wsd = sdc11073.wsdiscovery.WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        # Create a new device
        self.location = sdc11073.location.SdcLocation(fac='tklx', poc='CU1', bed='Bed')
        self.sdc_device = SomeDevice.from_mdib_file(self.wsd, None, '70041_MDIB_Final.xml')
        self._locValidators = [sdc11073.pmtypes.InstanceIdentifier('Validator', extension_string='System')]

    def tearDown(self):
        # close
        self.sdc_client.stop_all()
        self.sdc_device.stop_all()
        time.sleep(1)
        self.wsd.stop()

    def _start_with_compression(self, compressionFlag):
        """ Starts Device and Client with correct settigns  """

        # start device with compression settings
        if compressionFlag is None:
            self.sdc_device.set_used_compression()
        else:
            self.sdc_device.set_used_compression(compressionFlag)

        self.sdc_device.start_all()
        self.sdc_device.set_location(self.location, self._locValidators)

        time.sleep(0.5)  # allow full init of devices

        # Connect a new client to the divece
        xAddr = self.sdc_device.get_xaddrs()
        self.sdc_client = SdcClient(xAddr[0],
                                         sdc_definitions=self.sdc_device.mdib.sdc_definitions,
                                         ssl_context=None,
                                         )
        if compressionFlag is None:
            self.sdc_client.set_used_compression()
        else:
            self.sdc_client.set_used_compression(compressionFlag)
        self.sdc_client.start_all()
        time.sleep(0.5)

        # Get http connection to execute the call
        self.getService = self.sdc_client.client('Set')
        self.soap_client = next(iter(self.sdc_client._soap_clients.values()))
        self.clientHttpCon = self.soap_client._http_connection

        self.xml = XML_REQ
        # Python 2 and 3 compatibility
        if not isinstance(XML_REQ, bytes):
            self.xml = XML_REQ.encode('utf-8')

    def test_no_compression(self):
        self._start_with_compression(None)

        self.xml = bytearray(self.xml)  # cast to bytes, required to bypass httplib checks for is str
        headers = {
            'Content-type': 'application/soap+xml',
            'user_agent': 'pysoap',
            'Connection': 'keep-alive',
            'Content-Length': str(len(self.xml))
        }

        headers = dict((str(k), str(v)) for k, v in headers.items())

        self.clientHttpCon.request('POST', self.getService._url.path, body=self.xml, headers=headers)

        # Verify response is not compressed
        response = self.clientHttpCon.getresponse()
        content = response.read()
        print(len(content))
        # if request was successful we will be able to parse the xml
        try:
            etree.fromstring(content)
        except:
            self.fail("Wrong xml syntax. Msg {}".format(content))

    def test_gzip_compression(self):
        # Create a compressed getMetadata request
        self._start_with_compression(GZIP)

        self.xml = compression.CompressionHandler.compress_payload(GZIP, self.xml)
        self.xml = bytearray(self.xml)  # cast to bytes, required to bypass httplib checks for is str
        headers = {
            'Content-type': 'application/soap+xml',
            'user_agent': 'pysoap',
            'Connection': 'keep-alive',
            'Content-Encoding': GZIP,
            'Accept-Encoding': 'gzip, x-lz4',
            'Content-Length': str(len(self.xml))
        }
        headers = dict((str(k), str(v)) for k, v in headers.items())
        self.clientHttpCon.request('POST', self.getService._url.path, body=self.xml, headers=headers)
        # Verify response is comressed
        response = self.clientHttpCon.getresponse()
        responseHeaders = {k.lower(): v for k, v in response.getheaders()}
        content = response.read()
        content = compression.CompressionHandler.decompress_payload(GZIP, content)

        self.assertIn('content-encoding', responseHeaders)
        try:
            etree.fromstring(content)
        except:
            self.fail("Wrong xml syntax. Msg {}".format(content))

    @unittest.skipIf(LZ4 not in compression.CompressionHandler.available_encodings, 'no lz4 module available')
    def test_lz4_compression(self):
        # Create a compressed getMetadata request
        self._start_with_compression(LZ4)

        self.xml = compression.CompressionHandler.compress_payload(LZ4, self.xml)
        self.xml = bytearray(self.xml)  # cast to bytes, required to bypass httplib checks for is str
        headers = {
            'Content-type': 'application/soap+xml',
            'user_agent': 'pysoap',
            'Connection': 'keep-alive',
            'Content-Encoding': LZ4,
            'Accept-Encoding': 'gzip, x-lz4',
            'Content-Length': str(len(self.xml))
        }
        headers = dict((str(k), str(v)) for k, v in headers.items())
        self.clientHttpCon.request('POST', self.getService._url.path, body=self.xml, headers=headers)
        # Verify response is comressed
        response = self.clientHttpCon.getresponse()
        responseHeaders = {k.lower(): v for k, v in response.getheaders()}
        content = response.read()
        content = compression.CompressionHandler.decompress_payload(LZ4, content)

        self.assertIn('content-encoding', responseHeaders)
        try:
            etree.fromstring(content)
        except:
            self.fail("Wrong xml syntax. Msg {}".format(content))

class Test_Compression_ParseHeader(unittest.TestCase):

    def test_parseHeader(self):
        result = compression.CompressionHandler.parse_header('gzip,lz4')
        self.assertEqual(result, ['gzip', 'lz4'])
        result = compression.CompressionHandler.parse_header('lz4, gzip')
        self.assertEqual(result, ['lz4', 'gzip'])
        result = compression.CompressionHandler.parse_header('lz4;q=1, gzip; q = 0.5')
        self.assertEqual(result, ['lz4', 'gzip'])
        result = compression.CompressionHandler.parse_header('lz4;q= 1, gzip; q=0.5')
        self.assertEqual(result, ['lz4', 'gzip'])
        result = compression.CompressionHandler.parse_header('lz4;q= 1, gzip')
        self.assertEqual(result, ['lz4', 'gzip'])
        result = compression.CompressionHandler.parse_header('gzip; q=0.9,lz4')
        self.assertEqual(result, ['lz4', 'gzip'])
        result = compression.CompressionHandler.parse_header('gzip,lz4; q=0.9')
        self.assertEqual(result, ['gzip', 'lz4'])



