import logging
import unittest

from sdc11073.definitions_sdc import SDC_v1_Definitions
from sdc11073.pysoap.msgreader import MessageReaderClient


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
        msg_reader = MessageReaderClient(SDC_v1_Definitions, logging.getLogger('test'))
        message_data = msg_reader.read_received_message(xml)
        env = message_data.p_msg

        # verify that document was parsed
        self.assertTrue(env.header_node is not None)
        self.assertTrue(env.body_node is not None)

        tmp = env.header_node.find('n:alertcontrol', {'n': 'http://example.org/alertcontrol'})
        self.assertTrue(tmp is not None)

        tmp = env.body_node.find('m:alert', {'m': 'http://example.org/alert'})
        self.assertTrue(tmp is not None)

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
        msg_reader = MessageReaderClient(SDC_v1_Definitions, logging.getLogger('test'))
        message_data = msg_reader.read_received_message(xml)
        env = message_data.p_msg
        # verify that document was parsed
        self.assertTrue(env.header_node is not None)
        self.assertTrue(env.body_node is not None)

        self.assertEqual(env.address.message_id, 'http://example.com/someuniquestring')
        self.assertEqual(env.address.addr_to, 'mailto:fabrikam@example.com')
        self.assertEqual(env.address.addr_from, None)
        self.assertEqual(env.address.reply_to.address, 'http://example.com/business/client1')
        self.assertEqual(env.address.fault_to.address, 'http://example.com/business/client2')
        self.assertEqual(env.address.action, 'http://example.com/fabrikam/mail/Delete')

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
        msg_reader = MessageReaderClient(SDC_v1_Definitions, logging.getLogger('test'))
        message_data = msg_reader.read_received_message(xml)
        dpws_data = msg_reader.read_get_metadata_response(message_data)

        self.assertEqual(dpws_data.this_model.manufacturer[None], 'ACME Manufacturing')
        self.assertEqual(dpws_data.this_model.model_name['en-GB'], 'ColourBeam 9')
        self.assertEqual(dpws_data.this_model.model_name['en-US'], 'ColorBeam 9')

        self.assertEqual(dpws_data.this_device.friendly_name['en-GB'], 'My Device')
        self.assertEqual(dpws_data.this_device.friendly_name['de-DE'], 'Meine Kiste')
        self.assertEqual(dpws_data.this_device.firmware_version, '12.2')
        self.assertEqual(dpws_data.this_device.serial_number, '123_abc')

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
        msg_reader = MessageReaderClient(SDC_v1_Definitions, logging.getLogger('test'))
        message_data = msg_reader.read_received_message(xml)
        dpws_data = msg_reader.read_get_metadata_response(message_data)

        hosted = dpws_data.relationship.hosted
        self.assertEqual(hosted['http://printer.example.org/imaging/PrintService'].endpoint_references[0].address,
                         'http://172.30.184.244/print')
        self.assertEqual(hosted['http://printer.example.org/imaging/PrintService'].endpoint_references[1].address,
                         'http://[fdaa:23]/print1')
        self.assertEqual(hosted['http://printer.example.org/imaging/PrintService'].types[0], 'img:PrintBasicPortType')
        self.assertEqual(hosted['http://printer.example.org/imaging/PrintService'].types[1],
                         'img:PrintAdvancedPortType')
        self.assertEqual(hosted['http://printer.example.org/imaging/PrintService'].service_id,
                         'http://printer.example.org/imaging/PrintService')

        host = dpws_data.relationship.host
        self.assertEqual(host.endpoint_references[0].address, 'http://172.30.184.244/host')
        self.assertEqual(host.endpoint_references[1].address, 'http://[fdaa:23]/host1')
        self.assertEqual(host.types[0], 'img:HostPortType')
        self.assertEqual(host.types[1], 'img:HostAdvancedPortType')
