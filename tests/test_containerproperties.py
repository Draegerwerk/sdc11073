"""Test container properties."""

import datetime
import unittest
from decimal import Decimal
from enum import Enum
from unittest import mock

from lxml import etree
from src.sdc11073.xml_types.xml_structure import NodeTextListProperty

from sdc11073.mdib.statecontainers import AllowedValuesType
from sdc11073.namespaces import docname_from_qname, text_to_qname
from sdc11073.xml_types import pm_qnames as pm
from sdc11073.xml_types.isoduration import UTC
from sdc11073.xml_types.pm_types import CodedValue
from sdc11073.xml_types.xml_structure import DateOfBirthProperty as DoB
from sdc11073.xml_types.xml_structure import (
    DecimalListAttributeProperty,
    EnumAttributeProperty,
    HandleRefListAttributeProperty,
    IntegerAttributeProperty,
    NodeEnumQNameProperty,
    NodeEnumTextProperty,
    NodeStringProperty,
    NodeTextQNameProperty,
    QNameAttributeProperty,
    StringAttributeProperty,
    SubElementProperty,
    SubElementWithSubElementListProperty,
)
from tests import utils


# pylint: disable=protected-access
class MyEnum(Enum):
    a = '1'
    b = '2'
    c = '3'


class DummyBase:
    def __init__(self):
        for prop in self.props():
            prop.init_instance_data(self)

    def props(self) -> list:
        """Return empty list."""
        return []

    def mk_node(self):  # noqa: ANN201
        """Make a node."""
        node = etree.Element('test')
        for prop in self.props():
            prop.update_xml_value(self, node)
        return node


class Dummy(DummyBase):
    str_prop1 = StringAttributeProperty(attribute_name='str_prop1', default_py_value='bar')
    str_prop2 = StringAttributeProperty(attribute_name='str_prop2', implied_py_value='foobar')
    str_prop3 = StringAttributeProperty(attribute_name='str_prop3')
    int_prop = IntegerAttributeProperty(attribute_name='int_prop')
    enum_prop = EnumAttributeProperty(attribute_name='enum_prop', enum_cls=MyEnum)
    str_list_attr = HandleRefListAttributeProperty(attribute_name='str_list_attr')
    dec_list_attr = DecimalListAttributeProperty(attribute_name='dec_list_attr')

    def props(self):
        yield self.__class__.str_prop1
        yield self.__class__.str_prop2
        yield self.__class__.str_prop3
        yield self.__class__.int_prop
        yield self.__class__.enum_prop
        yield self.__class__.str_list_attr
        yield self.__class__.dec_list_attr


class DummyNodeText(DummyBase):
    node_text_mand = NodeStringProperty(etree.QName('pref', 'node_text_mand'), default_py_value='foo', min_length=1)
    node_text_opt = NodeStringProperty(etree.QName('pref', 'node_text_opt'), implied_py_value='bar', is_optional=True)

    def props(self):
        yield self.__class__.node_text_mand
        yield self.__class__.node_text_opt


class DummyNodeEnumText(DummyBase):
    node_text_mand = NodeEnumTextProperty(etree.QName('pref', 'node_text_mand'), MyEnum, default_py_value=MyEnum.a)
    node_text_opt = NodeEnumTextProperty(etree.QName('pref', 'node_text_opt'), MyEnum, is_optional=True)
    node_text_mand_no_default = NodeEnumTextProperty(etree.QName('pref', 'node_text_mand_no_default'), MyEnum)

    def props(self):
        yield self.__class__.node_text_mand
        yield self.__class__.node_text_opt
        yield self.__class__.node_text_mand_no_default


class DummySubElement(DummyBase):
    sub_elem_mand = SubElementProperty(
        etree.QName('pref', 'sub_elem_mand'),
        value_class=CodedValue,
        default_py_value=CodedValue('foo'),
    )
    sub_elem_opt = SubElementProperty(etree.QName('pref', 'sub_elem_opt'), value_class=CodedValue, is_optional=True)

    def props(self):
        yield self.__class__.sub_elem_mand
        yield self.__class__.sub_elem_opt


class DummySubElementList(DummyBase):
    sub_elem = SubElementWithSubElementListProperty(
        etree.QName('pref', 'sub_elem'),
        default_py_value=AllowedValuesType(),
        value_class=AllowedValuesType,
    )

    def props(self):
        yield self.__class__.sub_elem


class TestContainerProperties(unittest.TestCase):
    def setUp(self):
        self.dummy = Dummy()

    def test_date_of_birth_regex(self):
        result = DoB.mk_value_object('2003-06-30')
        self.assertEqual(result, datetime.date(2003, 6, 30))

        for text in ('foo', '0000-06-30', '01-00-01', '01-01-00'):  # several invalid strings
            result = DoB.mk_value_object(text)
            self.assertTrue(result is None, msg=f'result of {text} should be None, but it is {result}')

        result = DoB.mk_value_object('2003-06-30T14:53:12.4')
        self.assertEqual(result, datetime.datetime(2003, 6, 30, 14, 53, 12, 400000))  # noqa: DTZ001
        self.assertEqual(result.tzinfo, None)

        # add time zone UTC
        result = DoB.mk_value_object('2003-06-30T15:53:12Z')
        self.assertEqual(result.second, 12)
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.tzinfo.utcoffset(0), datetime.timedelta(0))

        # add time zone UTC
        result = DoB.mk_value_object('2003-06-30T15:53:12.4Z')
        self.assertEqual(result.second, 12)
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.tzinfo.utcoffset(0), datetime.timedelta(0))

        # add time zone +6hours
        result = DoB.mk_value_object('2003-06-30T15:53:12.4+6:02')
        self.assertEqual(result.second, 12)
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.tzinfo.utcoffset(0), datetime.timedelta(minutes=60 * 6 + 2))

        # add time zone -3hours
        result = DoB.mk_value_object('2003-06-30T15:53:12.4-03:01')
        self.assertEqual(result.second, 12)
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.tzinfo.utcoffset(0), datetime.timedelta(minutes=(30 * 6 + 1) * -1))

    def test_date_of_birth_to_string(self):
        date_string = DoB._mk_datestring(datetime.date(2004, 3, 6))
        self.assertEqual(date_string, '2004-03-06')

        date_string = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16))  # noqa: DTZ001
        self.assertEqual(date_string, '2004-03-06T14:15:16')

        date_string = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 4, 5, 6))  # noqa: DTZ001
        self.assertEqual(date_string, '2004-03-06T04:05:06')  # verify leading zeros in date and time

        date_string = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000))  # noqa: DTZ001
        self.assertEqual(date_string, '2004-03-06T14:15:16.7')

        date_string = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=UTC(0, 'UTC')))
        self.assertEqual(date_string, '2004-03-06T14:15:16.7Z')

        date_string = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=UTC(180, 'UTC+1')))
        self.assertEqual(date_string, '2004-03-06T14:15:16.7+03:00')

        date_string = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=UTC(-120, 'UTC+1')))
        self.assertEqual(date_string, '2004-03-06T14:15:16.7-02:00')

        date_string = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=UTC(181, 'UTC+1')))
        self.assertEqual(date_string, '2004-03-06T14:15:16.7+03:01')

        date_string = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=UTC(-121, 'UTC+1')))
        self.assertEqual(date_string, '2004-03-06T14:15:16.7-02:01')

    def test_attribute_property_base(self):
        dummy = Dummy()

        # verify that default value is initially set
        self.assertEqual(dummy.str_prop1, 'bar')
        self.assertEqual(Dummy.str_prop1.get_actual_value(dummy), 'bar')

        self.assertEqual(dummy.str_prop2, 'foobar')
        self.assertIsNone(Dummy.str_prop2.get_actual_value(dummy))

        self.assertIsNone(dummy.str_prop3)
        self.assertIsNone(Dummy.str_prop3.get_actual_value(dummy))

        node = dummy.mk_node()
        self.assertEqual(node.attrib['str_prop1'], 'bar')
        self.assertFalse('str_prop2' in node.attrib)
        self.assertFalse('str_prop3' in node.attrib)
        self.assertFalse('int_prop' in node.attrib)
        self.assertFalse('enum_prop' in node.attrib)
        self.assertFalse('str_list_attr' in node.attrib)

        # verify that new value is set
        dummy.str_prop1 = 'hello'
        self.assertEqual(dummy.str_prop1, 'hello')
        self.assertEqual(Dummy.str_prop1.get_actual_value(dummy), 'hello')

        dummy.str_prop1 = None
        self.assertEqual(dummy.str_prop1, None)
        self.assertEqual(Dummy.str_prop1.get_actual_value(dummy), None)

        node = dummy.mk_node()
        self.assertFalse('str_prop1' in node.attrib)

        self.assertEqual(dummy.str_prop2, 'foobar')
        dummy.str_prop2 = None
        self.assertEqual(dummy.str_prop2, 'foobar')

        self.assertEqual(dummy.str_prop3, None)

    def test_string_attribute_property(self):
        dummy = Dummy()
        for value in (42, 42.42, True, b'hello'):
            try:
                dummy.str_prop1 = value
            except ValueError:  # noqa: PERF203
                pass
            else:
                raise Exception(f'dummy.prop1 = {value} did not raise ValueError!')  # noqa: EM102, TRY002

    def test_enum_attribute_property(self):
        dummy = Dummy()
        for value in (1, 2, 42, 42.42, True, b'hello'):
            try:
                dummy.enum_prop = value
            except ValueError:  # noqa: PERF203
                pass
            else:
                raise Exception(f'dummy.prop1 = {value} did not raise ValueError!')  # noqa: EM102, TRY002
        for value in MyEnum:
            dummy.enum_prop = value
            self.assertEqual(dummy.enum_prop, value)
            node = dummy.mk_node()
            self.assertEqual(node.attrib['enum_prop'], value.value)

    def test_node_attribute_list_property_base(self):
        dummy = Dummy()
        self.assertEqual(Dummy.str_list_attr.get_actual_value(dummy), [])

        self.assertEqual(dummy.str_list_attr, [])
        dummy.str_list_attr = ['a', 'b']
        self.assertEqual(dummy.str_list_attr, ['a', 'b'])
        node = dummy.mk_node()
        self.assertEqual(node.attrib['str_list_attr'], 'a b')

        for value in (1, 2, ['42', 43], 42.42, True, b'hello'):
            try:
                dummy.str_list_attr = value
            except ValueError:  # noqa: PERF203
                pass
            else:
                raise Exception(f'dummy.str_list_attr = {value} did not raise ValueError!')  # noqa: EM102, TRY002

    def test_decimal_list_attribute_property(self):
        dummy = Dummy()
        self.assertEqual(Dummy.dec_list_attr.get_actual_value(dummy), [])

        self.assertEqual(dummy.dec_list_attr, [])
        dummy.dec_list_attr = [Decimal('1.11'), Decimal('.99')]
        self.assertEqual(dummy.dec_list_attr, [Decimal('1.11'), Decimal('.99')])

        node = dummy.mk_node()
        self.assertEqual(node.attrib['dec_list_attr'], '1.11 0.99')

        for value in (1, 2, ['42', 43], 42.42, True, b'hello'):
            try:
                dummy.dec_list_attr = value
            except ValueError:  # noqa: PERF203
                pass
            else:
                raise Exception(f'dummy.str_list_attr = {value} did not raise ValueError!')  # noqa: EM102, TRY002

    def test_node_text_property(self):
        dummy = DummyNodeText()
        self.assertEqual(dummy.node_text_mand, 'foo')
        self.assertEqual(dummy.node_text_opt, 'bar')
        self.assertEqual(DummyNodeText.node_text_mand.get_actual_value(dummy), 'foo')
        self.assertEqual(DummyNodeText.node_text_opt.get_actual_value(dummy), None)
        dummy.node_text_mand = None
        self.assertRaises(
            ValueError,
            dummy.mk_node,
        )  # implied value does not help here, we need a real value for mand. prop.
        dummy.node_text_mand = 'foo'
        node = dummy.mk_node()
        self.assertEqual(1, len(node))  # the empty optional node is not added, only the mandatory one
        self.assertEqual(node[0].tag, '{pref}node_text_mand')
        self.assertEqual(node[0].text, 'foo')

        dummy.node_text_opt = 'foobar'
        self.assertEqual(dummy.node_text_opt, 'foobar')
        self.assertEqual(DummyNodeText.node_text_opt.get_actual_value(dummy), 'foobar')
        node = dummy.mk_node()
        self.assertEqual(2, len(node))
        self.assertEqual(node[0].text, 'foo')
        self.assertEqual(node[1].text, 'foobar')

        dummy.node_text_mand = 'hello'
        node = dummy.mk_node()
        self.assertEqual(2, len(node))
        self.assertEqual(node[0].text, 'hello')
        self.assertEqual(node[1].text, 'foobar')
        for value in (42, b'hello'):
            try:
                dummy.node_text_mand = value
            except ValueError:  # noqa: PERF203
                pass
            else:
                raise Exception(f'dummy.node_text_mand = {value} did not raise ValueError!')  # noqa: EM102, TRY002

    def test_dummy_node_enum_text(self):
        dummy = DummyNodeEnumText()
        # verify that mandatory element without value raises a ValueError
        self.assertRaises(ValueError, dummy.mk_node)
        dummy.node_text_mand_no_default = MyEnum.c
        node = dummy.mk_node()
        self.assertEqual(dummy.node_text_mand, MyEnum.a)
        self.assertEqual(dummy.node_text_opt, None)
        self.assertEqual(DummyNodeEnumText.node_text_mand.get_actual_value(dummy), MyEnum.a)
        self.assertEqual(DummyNodeEnumText.node_text_opt.get_actual_value(dummy), None)
        node = dummy.mk_node()
        self.assertEqual(2, len(node))  # the empty optional node is not added, only the mandatory ones
        self.assertEqual(node[0].tag, '{pref}node_text_mand')
        self.assertEqual(node[0].text, MyEnum.a.value)

        dummy.node_text_opt = MyEnum.b
        self.assertEqual(dummy.node_text_opt, MyEnum.b)
        self.assertEqual(DummyNodeEnumText.node_text_opt.get_actual_value(dummy), MyEnum.b)
        node = dummy.mk_node()
        self.assertEqual(3, len(node))
        self.assertEqual(node[0].text, MyEnum.a.value)
        self.assertEqual(node[1].text, MyEnum.b.value)
        self.assertEqual(node[2].text, MyEnum.c.value)

        dummy.node_text_mand = MyEnum.a
        node = dummy.mk_node()
        self.assertEqual(3, len(node))
        self.assertEqual(node[0].text, MyEnum.a.value)
        self.assertEqual(node[1].text, MyEnum.b.value)
        self.assertEqual(node[2].text, MyEnum.c.value)
        for value in (42, b'hello'):
            try:
                dummy.node_text_mand = value
            except ValueError:  # noqa: PERF203
                pass
            else:
                raise Exception(f'dummy.node_text_mand = {value} did not raise ValueError!')  # noqa: EM102, TRY002

    def test_sub_element_property(self):
        dummy = DummySubElement()
        self.assertEqual(dummy.sub_elem_mand, CodedValue('foo'))
        self.assertEqual(DummySubElement.sub_elem_mand.get_actual_value(dummy), CodedValue('foo'))

        self.assertEqual(dummy.sub_elem_opt, None)
        self.assertEqual(DummySubElement.sub_elem_opt.get_actual_value(dummy), None)

        dummy.sub_elem_mand = None
        self.assertRaises(ValueError, dummy.mk_node)  # mand. prop has no value

        dummy.sub_elem_mand = CodedValue('hello_again')
        node = dummy.mk_node()
        self.assertEqual(1, len(node))  # the empty optional node is not added, only the mandatory one
        self.assertEqual(node[0].tag, '{pref}sub_elem_mand')
        self.assertEqual(node[0].text, None)
        self.assertEqual(1, len(node[0].attrib))

        dummy.sub_elem_opt = CodedValue('foo', 'bar')
        self.assertEqual(dummy.sub_elem_opt, CodedValue('foo', 'bar'))
        self.assertEqual(DummySubElement.sub_elem_opt.get_actual_value(dummy), CodedValue('foo', 'bar'))
        node = dummy.mk_node()
        self.assertEqual(2, len(node))
        self.assertEqual(node[0].text, None)
        self.assertEqual(node[1].text, None)
        self.assertEqual(2, len(node[1].attrib))
        self.assertEqual(node[1].attrib['Code'], 'foo')
        self.assertEqual(node[1].attrib['CodingSystem'], 'bar')

    def test_sub_element_with_sub_element_list_property(self):
        dummy = DummySubElementList()
        self.assertEqual(dummy.sub_elem.Value, [])

        node = dummy.mk_node()
        self.assertEqual(0, len(node))  # the empty optional node is not added, only the mandatory one

        dummy.sub_elem.Value.append('42')
        dummy.sub_elem.Value.append('43')
        node = dummy.mk_node()
        self.assertEqual(1, len(node))  # the empty optional node is not added, only the mandatory one

        self.assertEqual(node[0].tag, '{pref}sub_elem')
        self.assertEqual(node[0].text, None)
        self.assertEqual(0, len(node[0].attrib))

        self.assertEqual(2, len(node[0]))
        self.assertEqual(node[0][0].tag, pm.Value)
        self.assertEqual(node[0][0].text, '42')
        self.assertEqual(node[0][1].tag, pm.Value)
        self.assertEqual(node[0][1].text, '43')


class TestQNameAttributeProperty(unittest.TestCase):
    def setUp(self):
        self.attribute_name = 'testAttribute'
        self.default_value = utils.random_qname()
        self.property = QNameAttributeProperty(
            attribute_name=self.attribute_name,
            default_py_value=self.default_value,
            is_optional=True,
        )

    def test_get_py_value_from_node(self):
        """Test with a valid QName attribute."""
        node = etree.Element('TestElement', nsmap={'ex': 'http://example.com'})
        node.set(self.attribute_name, 'ex:validValue')
        result = self.property.get_py_value_from_node(None, node)
        expected = text_to_qname('ex:validValue', node.nsmap)
        self.assertEqual(result, expected)

        # Test with a missing attribute
        node = etree.Element('TestElement')
        result = self.property.get_py_value_from_node(None, node)
        self.assertIsNone(result)

    def test_update_xml_value(self):
        """Test setting a valid QName value."""
        node = etree.Element('TestElement', nsmap={'ex': 'https://example.com'})
        instance = type('MockInstance', (object,), {})()
        setattr(instance, self.property._local_var_name, etree.QName('https://example.com', 'newValue'))
        self.property.update_xml_value(instance, node)
        self.assertEqual(node.get(self.attribute_name), 'ex:newValue')

        # Test removing the attribute when value is None
        setattr(instance, self.property._local_var_name, None)
        self.property._default_py_value = None
        self.property.update_xml_value(instance, node)
        self.assertNotIn(self.attribute_name, node.attrib)

    def test_default_value(self):
        """Test default value is used when no value is set."""
        value = self.property.__get__(None, None)
        self.assertEqual(value._default_py_value, self.default_value)


class TestNodeTextListProperty(unittest.TestCase):
    def setUp(self):
        self.sub_element_name = utils.random_qname()
        self.property = NodeTextListProperty(
            sub_element_name=self.sub_element_name,
            value_class=str,
        )
        self.instance = type('TestInstance', (object,), {})()
        self.node = etree.Element('Root', nsmap={self.sub_element_name.localname: self.sub_element_name.namespace})

    def test_update_xml_value_with_empty_list(self):
        setattr(self.instance, self.property._local_var_name, [])
        self.property.update_xml_value(self.instance, self.node)
        sub_node = self.node.find(self.sub_element_name.text)
        self.assertEqual(sub_node.text, '')

    def test_update_xml_value_with_values(self):
        setattr(self.instance, self.property._local_var_name, ['value1', 'value2'])
        self.property.update_xml_value(self.instance, self.node)
        sub_nodes = self.node.findall(self.sub_element_name.text)
        self.assertEqual(sub_nodes[0].text, 'value1 value2')

    def test_update_xml_value_with_none(self):
        setattr(self.instance, self.property._local_var_name, None)
        with self.assertRaises(ValueError, msg=f'mandatory value {self.sub_element_name.text} missing'):
            self.property.update_xml_value(self.instance, self.node)


class TestNodeTextQNameProperty(unittest.TestCase):
    def setUp(self):
        self.sub_element_name = utils.random_qname()
        self.default_value = utils.random_qname(localname=self.sub_element_name.localname)
        self.property = NodeTextQNameProperty(
            sub_element_name=self.sub_element_name, default_py_value=self.default_value, is_optional=True,
        )
        self.node = etree.Element('Root', nsmap={self.sub_element_name.localname: self.sub_element_name.namespace})

    def test_get_py_value_from_node(self):
        # Test with a valid QName in the XML node
        sub_node = etree.SubElement(self.node, self.sub_element_name)
        sub_node.text = f'{self.sub_element_name.localname}:{utils.random_qname_part()}'
        result = self.property.get_py_value_from_node(None, self.node)
        expected = text_to_qname(sub_node.text, sub_node.nsmap)
        self.assertEqual(result, expected)

        # Test with no sub-element present
        self.node.remove(sub_node)
        result = self.property.get_py_value_from_node(None, self.node)
        self.assertEqual(result, self.default_value)

    def test_update_xml_value(self):
        """Test setting a valid QName value."""
        instance = type('TestInstance', (object,), {})()
        new_value = utils.random_qname(localname=self.sub_element_name.text)
        setattr(instance, self.property._local_var_name, new_value)
        self.property.update_xml_value(instance, self.node)
        sub_node = self.node.find(self.sub_element_name)
        self.assertIsNotNone(sub_node)
        self.assertEqual(sub_node.text, docname_from_qname(new_value, self.node.nsmap))

        # Test removing the sub-element when value is None
        setattr(instance, self.property._local_var_name, None)
        self.property.update_xml_value(instance, self.node)
        sub_node = self.node.find(self.sub_element_name)
        self.assertIsNone(sub_node)


class TestNodeEnumQNameProperty(unittest.TestCase):
    def setUp(self):
        self.q_name = utils.random_qname()
        self.sub_node_qname = etree.QName(self.q_name.text, utils.random_qname_part())
        self.sub_node_qname = utils.random_qname(localname=self.q_name.localname)
        self.property = NodeEnumQNameProperty(
            sub_element_name=self.sub_node_qname,
            enum_cls=mock.MagicMock(),
            default_py_value=None,
            implied_py_value=None,
            is_optional=True,
        )
        self.instance = mock.MagicMock()
        self.node = etree.Element('Root', nsmap={self.q_name.localname: self.q_name.namespace})

    def test_update_xml_value_with_none(self):
        setattr(self.instance, self.property._local_var_name, None)
        self.property.update_xml_value(self.instance, self.node)
        sub_node = self.node.find(self.sub_node_qname.text, namespaces=self.node.nsmap)
        self.assertIsNone(sub_node)

    def test_update_xml_value_with_value(self):
        mock_value = mock.MagicMock()
        mock_value.value = self.q_name
        setattr(self.instance, self.property._local_var_name, mock_value)
        self.property.update_xml_value(self.instance, self.node)
        sub_node = self.node.find(self.sub_node_qname.text, namespaces=self.node.nsmap)
        self.assertIsNotNone(sub_node)
        self.assertEqual(sub_node.text, docname_from_qname(self.q_name, self.node.nsmap))

    def test_update_xml_value_with_missing_sub_element(self):
        setattr(self.instance, self.property._local_var_name, None)
        self.property.update_xml_value(self.instance, self.node)
        sub_node = self.node.find(self.sub_node_qname.text, namespaces=self.node.nsmap)
        self.assertIsNone(sub_node)
