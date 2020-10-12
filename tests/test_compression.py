import unittest
import time
import sdc11073
from tests.mockstuff import SomeDevice
from sdc11073.sdcclient import SdcClient
from lxml import etree
import sdc11073.compression as compression

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

class Test_Compression(unittest.TestCase):

    def setUp(self):
        # Start discovery
        self.wsd = sdc11073.wsdiscovery.WSDiscoveryWhitelist(['127.0.0.1'])
        self.wsd.start()
        # Create a new device
        self.location = sdc11073.location.SdcLocation(fac='tklx', poc='CU1', bed='Bed')
        self.sdcDevice_Final = SomeDevice.fromMdibFile(self.wsd, None, '70041_MDIB_Final.xml')
        self._locValidators = [sdc11073.pmtypes.InstanceIdentifier('Validator', extensionString='System')]

    def tearDown(self):
        # close
        self.sdcClient_Final.stopAll()
        self.sdcDevice_Final.stopAll()
        time.sleep(1)
        self.wsd.stop()

    def _start_with_compression(self, compressionFlag):
        """ Starts Device and Client with correct settigns  """

        # start device with compression settings
        if compressionFlag is None:
            self.sdcDevice_Final.setUsedCompression()
        else:
            self.sdcDevice_Final.setUsedCompression(compressionFlag)

        self.sdcDevice_Final.startAll()
        self.sdcDevice_Final.setLocation(self.location, self._locValidators)

        time.sleep(0.5)  # allow full init of devices

        # Connect a new client to the divece
        xAddr = self.sdcDevice_Final.getXAddrs()
        self.sdcClient_Final = SdcClient(xAddr[0], deviceType=self.sdcDevice_Final.mdib.sdc_definitions.MedicalDeviceType)
        if compressionFlag is None:
            self.sdcClient_Final.setUsedCompression()
        else:
            self.sdcClient_Final.setUsedCompression(compressionFlag)
        self.sdcClient_Final.startAll()
        time.sleep(0.5)

        # Get http connection to execute the call
        self.getService = self.sdcClient_Final.client('Set')
        self.soapClient = next(iter(self.sdcClient_Final._soapClients.values()))
        self.clientHttpCon = self.soapClient._httpConnection

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
        self._start_with_compression(compression.GZIP)

        self.xml = self.soapClient.compressPayload(compression.GZIP, self.xml)
        self.xml = bytearray(self.xml)  # cast to bytes, required to bypass httplib checks for is str
        headers = {
            'Content-type': 'application/soap+xml',
            'user_agent': 'pysoap',
            'Connection': 'keep-alive',
            'Content-Encoding': compression.GZIP,
            'Accept-Encoding': 'gzip, x-lz4',
            'Content-Length': str(len(self.xml))
        }
        headers = dict((str(k), str(v)) for k, v in headers.items())
        self.clientHttpCon.request('POST', self.getService._url.path, body=self.xml, headers=headers)
        # Verify response is comressed
        response = self.clientHttpCon.getresponse()
        responseHeaders = {k.lower(): v for k, v in response.getheaders()}
        content = response.read()
        content = self.soapClient.decompress(content, compression.GZIP)

        self.assertIn('content-encoding', responseHeaders)
        try:
            etree.fromstring(content)
        except:
            self.fail("Wrong xml syntax. Msg {}".format(content))

    @unittest.skipIf(compression.LZ4 not in compression.encodings, 'no lz4 module available')
    def test_lz4_compression(self):
        # Create a compressed getMetadata request
        self._start_with_compression(compression.LZ4)

        self.xml = self.soapClient.compressPayload(compression.LZ4, self.xml)
        self.xml = bytearray(self.xml)  # cast to bytes, required to bypass httplib checks for is str
        headers = {
            'Content-type': 'application/soap+xml',
            'user_agent': 'pysoap',
            'Connection': 'keep-alive',
            'Content-Encoding': compression.LZ4,
            'Accept-Encoding': 'gzip, x-lz4',
            'Content-Length': str(len(self.xml))
        }
        headers = dict((str(k), str(v)) for k, v in headers.items())
        self.clientHttpCon.request('POST', self.getService._url.path, body=self.xml, headers=headers)
        # Verify response is comressed
        response = self.clientHttpCon.getresponse()
        responseHeaders = {k.lower(): v for k, v in response.getheaders()}
        content = response.read()
        content = self.soapClient.decompress(content, compression.LZ4)

        self.assertIn('content-encoding', responseHeaders)
        try:
            etree.fromstring(content)
        except:
            self.fail("Wrong xml syntax. Msg {}".format(content))

class Test_Compression_ParseHeader(unittest.TestCase):

    def test_parseHeader(self):
        result = compression.CompressionHandler.parseHeader('gzip,lz4')
        self.assertEqual(result, ['gzip', 'lz4'])
        result = compression.CompressionHandler.parseHeader('lz4, gzip')
        self.assertEqual(result, ['lz4', 'gzip'])
        result = compression.CompressionHandler.parseHeader('lz4;q=1, gzip; q = 0.5')
        self.assertEqual(result, ['lz4', 'gzip'])
        result = compression.CompressionHandler.parseHeader('lz4;q= 1, gzip; q=0.5')
        self.assertEqual(result, ['lz4', 'gzip'])
        result = compression.CompressionHandler.parseHeader('lz4;q= 1, gzip')
        self.assertEqual(result, ['lz4', 'gzip'])
        result = compression.CompressionHandler.parseHeader('gzip; q=0.9,lz4')
        self.assertEqual(result, ['lz4', 'gzip'])
        result = compression.CompressionHandler.parseHeader('gzip,lz4; q=0.9')
        self.assertEqual(result, ['gzip', 'lz4'])



