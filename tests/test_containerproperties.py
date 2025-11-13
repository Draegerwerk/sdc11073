"""Test container properties."""

import datetime
import unittest
import uuid
from decimal import Decimal
from enum import Enum
from unittest import mock

from lxml import etree
from src.sdc11073.xml_types.xml_structure import ContainerProperty, NodeTextQNameListProperty

from sdc11073 import xml_utils
from sdc11073.mdib.statecontainers import AllowedValuesType
from sdc11073.namespaces import docname_from_qname, text_to_qname
from sdc11073.xml_types import pm_qnames as pm
from sdc11073.xml_types.pm_types import CodedValue
from sdc11073.xml_types.xml_structure import (
    AnyEtreeNodeProperty,
    DecimalListAttributeProperty,
    EnumAttributeProperty,
    HandleRefListAttributeProperty,
    IntegerAttributeProperty,
    NodeEnumQNameProperty,
    NodeEnumTextProperty,
    NodeStringProperty,
    NodeTextListProperty,
    NodeTextQNameProperty,
    QNameAttributeProperty,
    StringAttributeProperty,
    SubElementProperty,
    SubElementWithSubElementListProperty,
    _AttributeListBase,
)
from sdc11073.xml_types.xml_structure import DateOfBirthProperty as DoB
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

        for text in ('foo', '00010-06-30', '01-00-01', '01-01-00'):  # several invalid strings
            result = DoB.mk_value_object(text)
            self.assertTrue(result is None, msg=f'result of {text} should be None, but it is {result}')

        result = DoB.mk_value_object('2003-06-30T14:53:12.4')
        self.assertEqual(result, datetime.datetime(2003, 6, 30, 14, 53, 12, 400000))  # noqa: DTZ001
        self.assertEqual(result.tzinfo, None)

        # add time zone UTC
        result = DoB.mk_value_object('2003-06-30T15:53:12Z')
        self.assertEqual(result.second, 12)
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.utcoffset(), datetime.timedelta(0))

        # add time zone UTC
        result = DoB.mk_value_object('2003-06-30T15:53:12.4Z')
        self.assertEqual(result.second, 12)
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.utcoffset(), datetime.timedelta(0))

        # add time zone +6hours
        result = DoB.mk_value_object('2003-06-30T15:53:12.4+6:02')
        self.assertEqual(result.second, 12)
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.utcoffset(), datetime.timedelta(minutes=60 * 6 + 2))

        # add time zone -3hours
        result = DoB.mk_value_object('2003-06-30T15:53:12.4-03:01')
        self.assertEqual(result.second, 12)
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.utcoffset(), datetime.timedelta(minutes=(30 * 6 + 1) * -1))

    def test_date_of_birth_to_string(self):
        date_string = DoB._mk_datestring(datetime.date(2004, 3, 6))
        self.assertEqual(date_string, '2004-03-06')

        date_string = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16))  # noqa: DTZ001
        self.assertEqual(date_string, '2004-03-06T14:15:16')

        date_string = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 4, 5, 6))  # noqa: DTZ001
        self.assertEqual(date_string, '2004-03-06T04:05:06')  # verify leading zeros in date and time

        date_string = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000))  # noqa: DTZ001
        self.assertEqual(date_string, '2004-03-06T14:15:16.7')

        date_string = DoB._mk_datestring(
            datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=datetime.timezone.utc),
        )
        self.assertEqual(date_string, '2004-03-06T14:15:16.7Z')

        date_string = DoB._mk_datestring(
            datetime.datetime(
                2004,
                3,
                6,
                14,
                15,
                16,
                700000,
                tzinfo=datetime.timezone(datetime.timedelta(minutes=180), 'UTC+1'),
            ),
        )
        self.assertEqual(date_string, '2004-03-06T14:15:16.7+03:00')

        date_string = DoB._mk_datestring(
            datetime.datetime(
                2004,
                3,
                6,
                14,
                15,
                16,
                700000,
                tzinfo=datetime.timezone(datetime.timedelta(minutes=-120), 'UTC+1'),
            ),
        )
        self.assertEqual(date_string, '2004-03-06T14:15:16.7-02:00')

        date_string = DoB._mk_datestring(
            datetime.datetime(
                2004,
                3,
                6,
                14,
                15,
                16,
                700000,
                tzinfo=datetime.timezone(datetime.timedelta(minutes=181), 'UTC+1'),
            ),
        )
        self.assertEqual(date_string, '2004-03-06T14:15:16.7+03:01')

        date_string = DoB._mk_datestring(
            datetime.datetime(
                2004,
                3,
                6,
                14,
                15,
                16,
                700000,
                tzinfo=datetime.timezone(datetime.timedelta(minutes=-121), 'UTC+1'),
            ),
        )
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


class TestContainerProperty(unittest.TestCase):
    def setUp(self):
        self.sub_element_name = utils.random_qname()
        self.property = ContainerProperty(
            sub_element_name=self.sub_element_name,
            value_class=mock.MagicMock(),
            cls_getter=mock.MagicMock(),
            ns_helper=mock.MagicMock(),
        )
        self.node = etree.Element('Root')
        self.instance = type('MockInstance', (object,), {})()

    def test_update_xml_value_with_none(self):
        setattr(self.instance, self.property._local_var_name, None)
        with self.assertRaises(ValueError, msg=f'mandatory value {self.sub_element_name} missing'):
            self.property.update_xml_value(self.instance, self.node)


class TestAnyEtreeNodeProperty(unittest.TestCase):
    def setUp(self):
        self.sub_element_name = utils.random_qname()
        self.property = AnyEtreeNodeProperty(sub_element_name=self.sub_element_name, is_optional=True)
        self.node = etree.Element('Root')
        self.instance = type('MockInstance', (object,), {})()

    def test_get_py_value_from_node_with_existing_sub_element(self):
        sub_node = etree.SubElement(self.node, self.sub_element_name)
        etree.SubElement(sub_node, 'Child1')
        etree.SubElement(sub_node, 'Child2')
        result = self.property.get_py_value_from_node(self.instance, self.node)
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0].tag, 'Child1')
        self.assertEqual(result[1].tag, 'Child2')

    def test_get_py_value_from_node_with_missing_sub_element(self):
        result = self.property.get_py_value_from_node(self.instance, self.node)
        self.assertIsNone(result)

    def test_update_xml_value_with_children(self):
        children = [etree.Element('Child1'), etree.Element('Child2')]
        setattr(self.instance, self.property._local_var_name, children)
        self.property.update_xml_value(self.instance, self.node)
        sub_node = self.node.find(self.sub_element_name)
        self.assertIsNotNone(sub_node)
        self.assertEqual(len(sub_node), 2)
        self.assertEqual(sub_node[0].tag, 'Child1')
        self.assertEqual(sub_node[1].tag, 'Child2')

    def test_update_xml_value_with_none(self):
        setattr(self.instance, self.property._local_var_name, None)
        self.property.update_xml_value(self.instance, self.node)
        sub_node = self.node.find(self.sub_element_name)
        self.assertIsNone(sub_node)

    def test_update_xml_value_with_mandatory_check(self):
        self.property._is_optional = False
        with self.assertRaises(ValueError, msg=f'mandatory value {self.sub_element_name} missing'):
            self.property.update_xml_value(self.instance, self.node)

    def test_get_py_value_from_node_with_invalid_node(self):
        invalid_node = etree.Element('InvalidRoot')
        self.assertIsNone(self.property.get_py_value_from_node(self.instance, invalid_node))

    def test_update_xml_value_removes_existing_sub_element(self):
        sub_node = etree.SubElement(self.node, self.sub_element_name)
        etree.SubElement(sub_node, 'Child1')
        setattr(self.instance, self.property._local_var_name, None)
        self.property.update_xml_value(self.instance, self.node)
        sub_node = self.node.find(self.sub_element_name)
        self.assertIsNone(sub_node)


class TestQNameAttributeProperty(unittest.TestCase):
    def setUp(self):
        self.attribute_name = utils.random_qname_part()
        self.default_value = utils.random_qname()
        self.property = QNameAttributeProperty(
            attribute_name=self.attribute_name,
            is_optional=True,
        )

    def test_get_py_value_from_node(self):
        """Test with a valid QName attribute."""
        node = etree.Element('TestElement', nsmap={'ex': 'http://example.com'})
        node.set(self.attribute_name, 'ex:validValue')
        result = self.property.get_py_value_from_node(None, node)
        expected = xml_utils.QName('{http://example.com}validValue')
        self.assertEqual(expected, result)
        self.assertIsInstance(result, xml_utils.QName)

        # Test with a missing attribute
        node = etree.Element('TestElement')
        result = self.property.get_py_value_from_node(None, node)
        self.assertIsNone(result)

    def test_update_xml_value(self):
        """Test setting a valid QName value."""
        node = etree.Element('TestElement')
        instance = type('MockInstance', (object,), {})()
        q_name = utils.random_qname()
        setattr(instance, self.property._local_var_name, q_name)
        self.property.update_xml_value(instance, node)
        self.assertEqual(node.get(self.attribute_name), docname_from_qname(q_name, node.nsmap))
        # ensure that the namespace of the qname has been added to the node
        self.assertIn(q_name.namespace, node.nsmap.values())

        # Test removing the attribute when value is None
        setattr(instance, self.property._local_var_name, None)
        self.property._default_py_value = None
        self.property.update_xml_value(instance, node)
        self.assertNotIn(self.attribute_name, node.attrib)


class TestNodeTextListProperty(unittest.TestCase):
    def setUp(self):
        self.sub_element_name = utils.random_qname()
        self.property = NodeTextListProperty(
            sub_element_name=self.sub_element_name,
            value_class=str,
        )
        self.instance = type('TestInstance', (object,), {})()
        self.node = etree.Element('Root')

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

        before_update = etree.tostring(self.node)
        self.property._is_optional = True
        etree.SubElement(self.node, self.sub_element_name)
        self.assertNotEqual(before_update, etree.tostring(self.node))
        self.property.update_xml_value(self.instance, self.node)
        self.assertEqual(before_update, etree.tostring(self.node))


class TestNodeTextQNameProperty(unittest.TestCase):
    def setUp(self):
        self.prefix = utils.random_qname_part()
        self.sub_element_name = utils.random_qname()
        self.property = NodeTextQNameProperty(
            sub_element_name=self.sub_element_name,
            is_optional=True,
        )
        self.node = etree.Element('Root', nsmap={self.prefix: self.sub_element_name.namespace})

    def test_get_py_value_from_node(self):
        # Test with a valid QName in the XML node
        sub_node = etree.SubElement(self.node, self.sub_element_name)
        sub_node.text = f'{self.prefix}:{utils.random_qname_part()}'
        result = self.property.get_py_value_from_node(None, self.node)
        expected = text_to_qname(sub_node.text, sub_node.nsmap)
        self.assertEqual(result, expected)

        # Test with no sub-element present
        self.node.remove(sub_node)
        result = self.property.get_py_value_from_node(None, self.node)
        self.assertIsNone(result)

    def test_update_xml_value(self):
        """Test setting a valid QName value."""
        instance = type('TestInstance', (object,), {})()
        new_value = utils.random_qname(namespace=self.sub_element_name.text)
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

    def test_update_xml_value_with_random_namespace(self):
        instance = type('TestInstance', (object,), {})()
        new_value = utils.random_qname()
        setattr(instance, self.property._local_var_name, new_value)
        self.property.update_xml_value(instance, self.node)
        sub_node = self.node.find(self.sub_element_name)
        self.assertIsNotNone(sub_node)
        self.assertEqual(sub_node.text, docname_from_qname(new_value, sub_node.nsmap))
        self.assertIn(new_value.namespace, sub_node.nsmap.values())

    def test_update_xml_value_with_none(self):
        instance = type('TestInstance', (object,), {})()
        self.property._is_optional = False
        setattr(instance, self.property._local_var_name, None)
        with self.assertRaises(ValueError, msg=f'mandatory value {self.sub_element_name.text} missing'):
            self.property.update_xml_value(instance, self.node)


class TestNodeEnumQNameProperty(unittest.TestCase):
    def setUp(self):
        self.prefix = utils.random_qname_part()
        self.sub_node_qname = utils.random_qname()
        self.property = NodeEnumQNameProperty(
            sub_element_name=self.sub_node_qname,
            enum_cls=mock.MagicMock(),
            default_py_value=None,
            implied_py_value=None,
            is_optional=True,
        )
        self.instance = mock.MagicMock()
        self.node = etree.Element('Root')

    def test_update_xml_value_with_none(self):
        setattr(self.instance, self.property._local_var_name, None)
        self.property.update_xml_value(self.instance, self.node)
        sub_node = self.node.find(self.sub_node_qname.text, namespaces=self.node.nsmap)
        self.assertIsNone(sub_node)

    def test_update_xml_value_with_value(self):
        mock_value = mock.MagicMock()
        mock_value.value = utils.random_qname()
        setattr(self.instance, self.property._local_var_name, mock_value)
        self.property.update_xml_value(self.instance, self.node)
        sub_node = self.node.find(self.sub_node_qname.text, namespaces=self.node.nsmap)
        self.assertIsNotNone(sub_node)
        self.assertEqual(sub_node.text, docname_from_qname(mock_value.value, sub_node.nsmap))


class TestNodeTextQNameListProperty(unittest.TestCase):
    def setUp(self):
        self.prefix = utils.random_qname_part()
        self.sub_element_name = utils.random_qname()
        self.property = NodeTextQNameListProperty(sub_element_name=self.sub_element_name)
        self.instance = mock.MagicMock()
        self.node = etree.Element('Root')

    def test_update_xml_value_with_none(self):
        """Test when the value is None and the property is optional."""
        self.property._is_optional = True
        setattr(self.instance, self.property._local_var_name, None)
        self.property.update_xml_value(self.instance, self.node)
        self.assertIsNone(self.node.find(self.sub_element_name.text))

    def test_update_xml_value_with_values(self):
        """Test when the value is a list of related QNames."""
        node = etree.Element('Root', nsmap={self.prefix: self.sub_element_name.namespace})
        qname1 = utils.random_qname(namespace=self.sub_element_name.text)
        qname2 = utils.random_qname(namespace=self.sub_element_name.text)
        setattr(self.instance, self.property._local_var_name, [qname1, qname2])
        self.property.update_xml_value(self.instance, node)
        sub_node = node.find(self.sub_element_name.text)
        self.assertIsNotNone(sub_node)
        self.assertEqual(sub_node.text, f'{self.prefix}:{qname1.localname} {self.prefix}:{qname2.localname}')

    def test_update_xml_value_with_random_qnames(self):
        """Test when the value is a list of random QNames."""
        qname1 = utils.random_qname()
        qname2 = utils.random_qname()
        setattr(self.instance, self.property._local_var_name, [qname1, qname2])
        self.property.update_xml_value(self.instance, self.node)
        sub_node = self.node.find(self.sub_element_name.text)
        self.assertIsNotNone(sub_node)
        q_name1_text, q_name2_text = sub_node.text.split()
        self.assertEqual(qname1, text_to_qname(q_name1_text, sub_node.nsmap))
        self.assertEqual(qname2, text_to_qname(q_name2_text, sub_node.nsmap))

    def test_update_xml_value_with_mandatory_check(self):
        """Test when the value is None and the property is mandatory."""
        self.property._is_optional = False
        setattr(self.instance, self.property._local_var_name, None)
        with self.assertRaises(ValueError, msg=f'mandatory value {self.sub_element_name} missing'):
            self.property.update_xml_value(self.instance, self.node)

    def test_update_xml_value_overwrites_existing(self):
        """Test that the method overwrites existing sub-element text."""
        existing_sub_node = etree.SubElement(self.node, self.sub_element_name)
        existing_sub_node.text = existing_sub_node.prefix + f':{utils.random_qname_part()}'
        qname = utils.random_qname()
        setattr(self.instance, self.property._local_var_name, [qname])
        self.property.update_xml_value(self.instance, self.node)
        sub_node = self.node.find(self.sub_element_name.text)
        self.assertEqual(sub_node.text, docname_from_qname(qname, ns_map=sub_node.nsmap))
        self.assertIn(qname.namespace, sub_node.nsmap.values())

    def test_update_xml_value_creates_sub_element(self):
        """Test that the method creates a sub-element if it doesn't exist."""
        qname = utils.random_qname(namespace=self.sub_element_name.text)
        setattr(self.instance, self.property._local_var_name, [qname])
        self.property.update_xml_value(self.instance, self.node)
        sub_node = self.node.find(self.sub_element_name.text)
        self.assertIsNotNone(sub_node)
        self.assertEqual(sub_node.text, docname_from_qname(qname, ns_map=sub_node.nsmap))


class TestAttributeListBase(unittest.TestCase):
    def setUp(self):
        self.attribute_name = utils.random_qname_part()
        value_converter = mock.MagicMock()
        value_converter.elem_to_xml = str
        self.property = _AttributeListBase(
            attribute_name=self.attribute_name,
            value_converter=value_converter,
        )
        self.instance = mock.MagicMock()
        self.node = etree.Element('Root')

    def test_update_xml_value_with_none(self):
        self.node.attrib[self.attribute_name] = uuid.uuid4().hex
        setattr(self.instance, self.property._local_var_name, None)
        self.property.update_xml_value(self.instance, self.node)
        self.assertIsNone(self.node.get(self.attribute_name))

        self.property._is_optional = False
        with self.assertRaises(ValueError, msg=f'mandatory value {self.attribute_name} missing'):
            self.property.update_xml_value(self.instance, self.node)

    def test_update_xml_value_with_value(self):
        mock_value = [uuid.uuid4().hex, uuid.uuid4().hex]
        setattr(self.instance, self.property._local_var_name, mock_value)
        self.property.update_xml_value(self.instance, self.node)
        self.assertEqual(' '.join(mock_value), self.node.get(self.attribute_name))
