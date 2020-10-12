import unittest
from sdc11073.pysoap import soapenvelope


class TestSoapEnvelope(unittest.TestCase):
    
    def test_soap12_fromXML(self):
        xml = b'''<?xml version="1.0" encoding="UTF-8"?><env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope">
 <env:Header>
  <n:alertcontrol xmlns:n="http://example.org/alertcontrol">
   <n:priority>1</n:priority>
   <n:expires>2001-06-22T14:00:00-05:00</n:expires>
  </n:alertcontrol>
 </env:Header>
 <env:Body>
  <m:alert xmlns:m="http://example.org/alert">
   <m:msg>Pick up Mary at school at 2pm</m:msg>
  </m:alert>
 </env:Body>
</env:Envelope>'''
        env = soapenvelope.ReceivedSoap12Envelope.fromXMLString(xml)
        
        # verify that document was parsed
        self.assertTrue(env.headerNode is not None)
        self.assertTrue(env.bodyNode is not None)
        
        tmp = env.headerNode.find('n:alertcontrol', {'n': 'http://example.org/alertcontrol'})
        self.assertTrue(tmp is not None)

        tmp = env.bodyNode.find('m:alert', {'m': 'http://example.org/alert'})
        self.assertTrue(tmp is not None)

        env = soapenvelope.ReceivedSoap12Envelope.fromXMLString(xml)
        
        # verify that document was 
        self.assertTrue(env.headerNode is not None)
        self.assertTrue(env.bodyNode is not None)


    def test_adressing_fromXML(self):
        xml = b'''<?xml version="1.0" encoding="UTF-8"?><env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://www.w3.org/2005/08/addressing">
 <env:Header>
    <wsa:MessageID>http://example.com/someuniquestring</wsa:MessageID>
    <wsa:ReplyTo>
      <wsa:Address>http://example.com/business/client1</wsa:Address>
    </wsa:ReplyTo>
    <wsa:FaultTo>
      <wsa:Address>http://example.com/business/client2</wsa:Address>
    </wsa:FaultTo>
    <wsa:To>mailto:fabrikam@example.com</wsa:To>
    <wsa:Action>http://example.com/fabrikam/mail/Delete</wsa:Action>
 </env:Header>
 <env:Body>
  <m:alert xmlns:m="http://example.org/alert">
   <m:msg>Pick up Mary at school at 2pm</m:msg>
  </m:alert>
 </env:Body>
</env:Envelope>'''
        env = soapenvelope.DPWSEnvelope.fromXMLString(xml)
        
        # verify that document was parsed
        self.assertTrue(env.headerNode is not None)
        self.assertTrue(env.bodyNode is not None)
        
        self.assertEqual(env.address.messageId, 'http://example.com/someuniquestring' )
        self.assertEqual(env.address.to, 'mailto:fabrikam@example.com' )
        self.assertEqual(env.address.from_, None )
        self.assertEqual(env.address.replyTo.address, 'http://example.com/business/client1' )
        self.assertEqual(env.address.faultTo.address, 'http://example.com/business/client2' )
        self.assertEqual(env.address.action, 'http://example.com/fabrikam/mail/Delete' )
        

    def test_DeviceCharacteristics_fromXML(self):
        xml = b'''<?xml version="1.0" encoding="UTF-8"?><env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope" 
           xmlns:wsa="http://www.w3.org/2005/08/addressing"
           xmlns:wsx="http://schemas.xmlsoap.org/ws/2004/09/mex"
           xmlns:dpws="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01">
 <env:Header>
    <wsa:MessageID>http://example.com/someuniquestring</wsa:MessageID>
    <wsa:ReplyTo>
      <wsa:Address>http://example.com/business/client1</wsa:Address>
    </wsa:ReplyTo>
    <wsa:FaultTo>
      <wsa:Address>http://example.com/business/client2</wsa:Address>
    </wsa:FaultTo>
    <wsa:To>mailto:fabrikam@example.com</wsa:To>
    <wsa:Action>http://example.com/fabrikam/mail/Delete</wsa:Action>
 </env:Header>
 <env:Body>
   <wsx:Metadata>
     <wsx:MetadataSection Dialect="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/ThisModel">
      <dpws:ThisModel>
        <dpws:Manufacturer>ACME Manufacturing</dpws:Manufacturer>
        <dpws:ModelName xml:lang="en-GB" >ColourBeam 9</dpws:ModelName>
        <dpws:ModelName xml:lang="en-US" >ColorBeam 9</dpws:ModelName>
      </dpws:ThisModel>
    </wsx:MetadataSection> 
     <wsx:MetadataSection Dialect="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/ThisDevice">
      <dpws:ThisDevice>
        <dpws:FriendlyName xml:lang="en-GB" >My Device</dpws:FriendlyName>
        <dpws:FriendlyName xml:lang="de-DE" >Meine Kiste</dpws:FriendlyName>
        <dpws:FirmwareVersion>12.2</dpws:FirmwareVersion>
        <dpws:SerialNumber>123_abc</dpws:SerialNumber>
      </dpws:ThisDevice>
    </wsx:MetadataSection> 
   </wsx:Metadata>
 </env:Body>
</env:Envelope>'''
        env = soapenvelope.DPWSEnvelope.fromXMLString(xml)
        
        # verify that document was parsed
        self.assertTrue(env.headerNode is not None)
        self.assertTrue(env.bodyNode is not None)
        
        self.assertEqual(env.thisModel.manufacturer[None], 'ACME Manufacturing' )
        self.assertEqual(env.thisModel.modelName['en-GB'], 'ColourBeam 9' )
        self.assertEqual(env.thisModel.modelName['en-US'], 'ColorBeam 9' )

        self.assertEqual(env.thisDevice.friendlyName['en-GB'], 'My Device' )
        self.assertEqual(env.thisDevice.friendlyName['de-DE'], 'Meine Kiste' )
        self.assertEqual(env.thisDevice.firmwareVersion, '12.2' )
        self.assertEqual(env.thisDevice.serialNumber, '123_abc' )



    def test_Hosting_fromXML(self):
        xml = b'''<?xml version="1.0" encoding="UTF-8"?>
        <env:Envelope xmlns:env="http://www.w3.org/2003/05/soap-envelope" 
           xmlns:wsa="http://www.w3.org/2005/08/addressing"
           xmlns:wsx="http://schemas.xmlsoap.org/ws/2004/09/mex"
           xmlns:dpws="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01">
 <env:Header>
    <wsa:MessageID>http://example.com/someuniquestring</wsa:MessageID>
    <wsa:ReplyTo>
      <wsa:Address>http://example.com/business/client1</wsa:Address>
    </wsa:ReplyTo>
    <wsa:FaultTo>
      <wsa:Address>http://example.com/business/client2</wsa:Address>
    </wsa:FaultTo>
    <wsa:To>mailto:fabrikam@example.com</wsa:To>
    <wsa:Action>http://example.com/fabrikam/mail/Delete</wsa:Action>
 </env:Header>
 <env:Body>
   <wsx:Metadata>
     <wsx:MetadataSection Dialect="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/Relationship">
      <dpws:Relationship Type="http://docs.oasis-open.org/ws-dd/ns/dpws/2009/01/host">
        <dpws:Host>
          <wsa:EndpointReference><wsa:Address>http://172.30.184.244/host</wsa:Address></wsa:EndpointReference>
          <wsa:EndpointReference><wsa:Address>http://[fdaa:23]/host1</wsa:Address></wsa:EndpointReference>
          <dpws:Types>img:HostPortType img:HostAdvancedPortType</dpws:Types>
        </dpws:Host>
        <dpws:Hosted>
          <wsa:EndpointReference><wsa:Address>http://172.30.184.244/print</wsa:Address></wsa:EndpointReference>
          <wsa:EndpointReference><wsa:Address>http://[fdaa:23]/print1</wsa:Address></wsa:EndpointReference>
          <dpws:Types>img:PrintBasicPortType img:PrintAdvancedPortType</dpws:Types>
          <dpws:ServiceId>http://printer.example.org/imaging/PrintService</dpws:ServiceId>
        </dpws:Hosted>
      </dpws:Relationship>
    </wsx:MetadataSection> 
   </wsx:Metadata>
 </env:Body>
</env:Envelope>'''
        env = soapenvelope.DPWSEnvelope.fromXMLString(xml)
        
        # verify that document was parsed
        self.assertTrue(env.headerNode is not None)
        self.assertTrue(env.bodyNode is not None)
        
        self.assertEqual(env.hosted['http://printer.example.org/imaging/PrintService'].endpointReferences[0].address, 'http://172.30.184.244/print')
        self.assertEqual(env.hosted['http://printer.example.org/imaging/PrintService'].endpointReferences[1].address, 'http://[fdaa:23]/print1')
        self.assertEqual(env.hosted['http://printer.example.org/imaging/PrintService'].types[0], 'img:PrintBasicPortType')
        self.assertEqual(env.hosted['http://printer.example.org/imaging/PrintService'].types[1], 'img:PrintAdvancedPortType')
        self.assertEqual(env.hosted['http://printer.example.org/imaging/PrintService'].serviceId, 'http://printer.example.org/imaging/PrintService')

        self.assertEqual(env.host.endpointReferences[0].address, 'http://172.30.184.244/host')
        self.assertEqual(env.host.endpointReferences[1].address, 'http://[fdaa:23]/host1')
        self.assertEqual(env.host.types[0], 'img:HostPortType')
        self.assertEqual(env.host.types[1], 'img:HostAdvancedPortType')


    def test_soap12_toXML(self):
        env = soapenvelope.Soap12Envelope({})
        print (env.as_xml())


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestSoapEnvelope)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
