import pathlib
import unittest
import uuid

from lxml import etree

from sdc11073.consumer.components import SdcConsumerComponents
from sdc11073.consumer.consumerimpl import SdcConsumer
from sdc11073.definitions_sdc import SdcV1Definitions
from sdc11073.exceptions import ValidationError
from sdc11073.namespaces import PrefixNamespace
from sdc11073.provider.components import SdcProviderComponents
from tests.mockstuff import SomeDevice

here = pathlib.Path(__file__).parent

# declaration of the foo schema
prefix_namespace_foo = PrefixNamespace('foo',
                                       "http://test/foo",
                                       "http://test/foo/foo_schema.xsd",
                                       here.joinpath('foo_schema.xsd'))

# a GetMdibResponse with a foo:Foo element in extension and a variable Bar attribute
mdib_data = """<?xml version='1.0' encoding='UTF-8'?>
<s12:Envelope 
xmlns:dom="http://standards.ieee.org/downloads/11073/11073-10207-2017/participant" 
xmlns:wsa="http://www.w3.org/2005/08/addressing" 
xmlns:s12="http://www.w3.org/2003/05/soap-envelope" 
xmlns:msg="http://standards.ieee.org/downloads/11073/11073-10207-2017/message">
<s12:Header>
<wsa:Action>http://standards.ieee.org/downloads/11073/11073-20701-2018/GetService/GetMdibResponse</wsa:Action>
<wsa:MessageID>urn:uuid:aab2485a-e114-4c3c-850c-6200c6ecb49b</wsa:MessageID>
<wsa:RelatesTo>urn:uuid:96a2eaf3-e14c-49a8-9d03-bb1c217a46b9</wsa:RelatesTo>
</s12:Header>
<s12:Body>
<msg:GetMdibResponse MdibVersion="243" SequenceId="urn:uuid:0ae28b40-19d2-458c-80c1-bf4198694572">
<msg:Mdib xmlns:ext="http://standards.ieee.org/downloads/11073/11073-10207-2017/extension" 
xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
xmlns:foo="http://test/foo"
MdibVersion="243" SequenceId="urn:uuid:0ae28b40-19d2-458c-80c1-bf4198694572">
<dom:MdDescription DescriptionVersion="0">
<dom:Mds Handle="mds_0" DescriptorVersion="4">
    <ext:Extension>
		<foo:Foo Bar="{}"/>
	</ext:Extension>
	<dom:Type Code="67108865">
	<dom:ConceptDescription Lang="en-US">SDPi Test MDS</dom:ConceptDescription>
	</dom:Type>
    <dom:AlertSystem Handle="alert_system.mds_0" SelfCheckPeriod="PT0H0M5S">
    </dom:AlertSystem>
	<dom:Sco Handle="sco.mds_0">
	</dom:Sco>
	<dom:MetaData/>
	<dom:SystemContext Handle="system_context.mds_0">
	    <dom:PatientContext Handle="patient_context.mds_0"/>
	    <dom:LocationContext Handle="location_context.mds_0"/>
	</dom:SystemContext>
	</dom:Mds>
</dom:MdDescription>
<dom:MdState StateVersion="0">
</dom:MdState>
</msg:Mdib>
</msg:GetMdibResponse>
</s12:Body>
</s12:Envelope>
"""


class TestAdditionalSchema(unittest.TestCase):

    def setUp(self):
        specific_components_consumer = SdcConsumerComponents(additional_schema_specs=[prefix_namespace_foo])
        specific_components_provider = SdcProviderComponents(additional_schema_specs=[prefix_namespace_foo])

        # instantiate a provider and a consumer.
        # It is not needed to start them, because msg_reader and msg_factory are created in constructor,
        # and these are all that's needed in this test.
        self.provider = SomeDevice.from_mdib_file(wsdiscovery=None,
                                                  epr=uuid.uuid4(),
                                                  specific_components=specific_components_provider,
                                                  mdib_xml_path='mdib_tns.xml')
        self.consumer = SdcConsumer('http://127.0.0.1:10000',  # exact value does not matter
                                    sdc_definitions=SdcV1Definitions,
                                    specific_components=specific_components_consumer,
                                    ssl_context_container=None,
                                    validate=True)

    def test_foo_consumer(self):
        # Verify that foo schema is known in msg_reader and msg_factory of consumer
        node = etree.Element(etree.QName("http://test/foo", 'Foo'))
        node.attrib['Bar'] = 'abcd'  # a valid value
        self.consumer.msg_reader._validate_node(node)
        self.consumer.msg_factory._validate_node(node)

        node.attrib['Bar'] = 'ab'  # value too short
        self.assertRaises(ValidationError, self.consumer.msg_reader._validate_node, node)
        self.assertRaises(ValidationError, self.consumer.msg_factory._validate_node, node)

    def test_foo_provider(self):
        # Verify that foo schema is known in msg_reader and msg_factory of provider
        node = etree.Element(etree.QName("http://test/foo", 'Foo'))
        node.attrib['Bar'] = 'abcd'  # a valid value
        self.provider.msg_reader._validate_node(node)
        self.provider.msg_factory._validate_node(node)
        node.attrib['Bar'] = 'ab'  # value too short
        self.assertRaises(ValidationError, self.provider.msg_reader._validate_node, node)
        self.assertRaises(ValidationError, self.provider.msg_factory._validate_node, node)

    def test_consumer(self):
        # Verify that Foo element in extension is validated.
        # It is sufficient to test only one of the validators, because they are all the same
        # in msg_factory and msg_reader of consumer and provider.
        self.consumer.msg_reader.read_received_message(mdib_data.format('abcs').encode('utf-8'))  # correct attribute
        self.assertRaises(ValidationError, self.consumer.msg_reader.read_received_message,
                          mdib_data.format('ab').encode('utf-8'))
