import datetime
import unittest
from decimal import Decimal
from enum import Enum

from lxml import etree as etree_

from sdc11073.isoduration import UTC
from sdc11073.mdib.containerproperties import DateOfBirthProperty as DoB
from sdc11073.mdib.containerproperties import NodeTextProperty, SubElementProperty, NodeEnumTextProperty
from sdc11073.mdib.containerproperties import SubElementWithSubElementListProperty
from sdc11073.mdib.containerproperties import HandleRefListAttributeProperty
from sdc11073.mdib.containerproperties import StringAttributeProperty, IntegerAttributeProperty, EnumAttributeProperty
from sdc11073.mdib.containerproperties import DecimalListAttributeProperty
from sdc11073.mdib.statecontainers import T_AllowedValues
from sdc11073.pmtypes import CodedValue
from sdc11073 import pm_qnames as pm


# pylint: disable=protected-access
class MyEnum(Enum):
    a = '1'
    b = '2'


class DummyBase:
    def __init__(self):
        for prop in self.props():
            prop.init_instance_data(self)

    def props(self):
        return []

    def mk_node(self):
        node = etree_.Element('test')
        for prop in self.props():
            prop.update_xml_value(self, node)
        return node


class Dummy(DummyBase):
    str_prop1 = StringAttributeProperty(attribute_name='str_prop1', default_py_value='bar', implied_py_value='foobar')
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
    node_text_mand = NodeTextProperty(etree_.QName('pref', 'node_text_mand'), implied_py_value='foo')
    node_text_opt = NodeTextProperty(etree_.QName('pref', 'node_text_opt'), implied_py_value='bar', is_optional=True)

    def props(self):
        yield self.__class__.node_text_mand
        yield self.__class__.node_text_opt


class DummyNodeEnumText(DummyBase):
    node_text_mand = NodeEnumTextProperty(MyEnum, etree_.QName('pref', 'node_text_mand'), implied_py_value=MyEnum.a)
    node_text_opt = NodeEnumTextProperty(MyEnum, etree_.QName('pref', 'node_text_opt'), is_optional=True)

    def props(self):
        yield self.__class__.node_text_mand
        yield self.__class__.node_text_opt


class DummySubElement(DummyBase):
    sub_elem_mand = SubElementProperty(etree_.QName('pref', 'sub_elem_mand'), value_class=CodedValue,
                                       implied_py_value=CodedValue('foo'))
    sub_elem_opt = SubElementProperty(etree_.QName('pref', 'sub_elem_opt'), value_class=CodedValue, is_optional=True)

    def props(self):
        yield self.__class__.sub_elem_mand
        yield self.__class__.sub_elem_opt


class DummySubElementList(DummyBase):
    sub_elem = SubElementWithSubElementListProperty(etree_.QName('pref', 'sub_elem'),
                                                    default_py_value=T_AllowedValues(),
                                                    value_class=T_AllowedValues)

    def props(self):
        yield self.__class__.sub_elem


class TestContainerProperties(unittest.TestCase):

    def setUp(self):

        self.dummy = Dummy()

    def test_DateOfBirthRegEx(self):
        result = DoB.mk_value_object('2003-06-30')
        self.assertEqual(result, datetime.date(2003, 6, 30))

        for text in ('foo', '0000-06-30', '01-00-01', '01-01-00'):  # several invalid strings
            result = DoB.mk_value_object(text)
            self.assertTrue(result is None, msg='result of {} should be None, but it is {}'.format(text, result))

        result = DoB.mk_value_object('2003-06-30T14:53:12.4')
        self.assertEqual(result, datetime.datetime(2003, 6, 30, 14, 53, 12, 400000))
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

    def test_DateOfBirth_toString(self):
        date_string = DoB._mk_datestring(datetime.date(2004, 3, 6))
        self.assertEqual(date_string, '2004-03-06')

        date_string = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16))
        self.assertEqual(date_string, '2004-03-06T14:15:16')

        date_string = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 4, 5, 6))
        self.assertEqual(date_string, '2004-03-06T04:05:06')  # verify leading zeros in date and time

        date_string = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000))
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

    def test_AttributePropertyBase(self):
        dummy = Dummy()

        # verify that default value is initially set
        self.assertEqual(Dummy.str_prop1.get_actual_value(dummy), 'bar')
        self.assertEqual(dummy.str_prop1, 'bar')

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
        # verify that implied value is returned when value is set to None
        dummy.str_prop1 = None
        self.assertEqual(dummy.str_prop1, 'foobar')
        self.assertEqual(Dummy.str_prop1.get_actual_value(dummy), None)

        node = dummy.mk_node()
        self.assertFalse('str_prop1' in node.attrib)

        self.assertEqual(dummy.str_prop2, 'foobar')
        dummy.str_prop2 = None
        self.assertEqual(dummy.str_prop2, 'foobar')

        self.assertEqual(dummy.str_prop3, None)

    def test_StringAttributeProperty(self):
        dummy = Dummy()
        for value in (42, 42.42, True, b'hello'):
            try:
                dummy.str_prop1 = value
            except ValueError:
                pass
            else:
                raise Exception(f'dummy.prop1 = {value} did not raise ValueError!')

    def test_EnumAttributeProperty(self):
        dummy = Dummy()
        for value in (1, 2, 42, 42.42, True, b'hello'):
            try:
                dummy.enum_prop = value
            except ValueError:
                pass
            else:
                raise Exception(f'dummy.prop1 = {value} did not raise ValueError!')
        for value in MyEnum:
            dummy.enum_prop = value
            self.assertEqual(dummy.enum_prop, value)
            node = dummy.mk_node()
            self.assertEqual(node.attrib['enum_prop'], value.value)

    def test_NodeAttributeListPropertyBase(self):
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
            except ValueError:
                pass
            else:
                raise Exception(f'dummy.str_list_attr = {value} did not raise ValueError!')

    def test_DecimalListAttributeProperty(self):
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
            except ValueError:
                pass
            else:
                raise Exception(f'dummy.str_list_attr = {value} did not raise ValueError!')

    def test_NodeTextProperty(self):
        dummy = DummyNodeText()
        self.assertEqual(dummy.node_text_mand, 'foo')
        self.assertEqual(dummy.node_text_opt, 'bar')
        self.assertEqual(DummyNodeText.node_text_mand.get_actual_value(dummy), None)
        self.assertEqual(DummyNodeText.node_text_opt.get_actual_value(dummy), None)
        node = dummy.mk_node()
        self.assertEqual(1, len(node))  # the empty optional node is not added, only the mandatory one
        self.assertEqual(node[0].tag, '{pref}node_text_mand')
        self.assertEqual(node[0].text, None)

        dummy.node_text_opt = 'foobar'
        self.assertEqual(dummy.node_text_opt, 'foobar')
        self.assertEqual(DummyNodeText.node_text_opt.get_actual_value(dummy), 'foobar')
        node = dummy.mk_node()
        self.assertEqual(2, len(node))
        self.assertEqual(node[0].text, None)
        self.assertEqual(node[1].text, 'foobar')

        dummy.node_text_mand = 'hello'
        node = dummy.mk_node()
        self.assertEqual(2, len(node))
        self.assertEqual(node[0].text, 'hello')
        self.assertEqual(node[1].text, 'foobar')
        for value in (42, b'hello'):
            try:
                dummy.node_text_mand = value
            except ValueError:
                pass
            else:
                raise Exception(f'dummy.node_text_mand = {value} did not raise ValueError!')

    def test_DummyNodeEnumText(self):
        dummy = DummyNodeEnumText()
        self.assertEqual(dummy.node_text_mand, MyEnum.a)
        self.assertEqual(dummy.node_text_opt, None)
        self.assertEqual(DummyNodeEnumText.node_text_mand.get_actual_value(dummy), None)
        self.assertEqual(DummyNodeEnumText.node_text_opt.get_actual_value(dummy), None)
        node = dummy.mk_node()
        self.assertEqual(1, len(node))  # the empty optional node is not added, only the mandatory one
        self.assertEqual(node[0].tag, '{pref}node_text_mand')
        self.assertEqual(node[0].text, None)

        dummy.node_text_opt = MyEnum.b
        self.assertEqual(dummy.node_text_opt, MyEnum.b)
        self.assertEqual(DummyNodeEnumText.node_text_opt.get_actual_value(dummy), MyEnum.b)
        node = dummy.mk_node()
        self.assertEqual(2, len(node))
        self.assertEqual(node[0].text, None)
        self.assertEqual(node[1].text, MyEnum.b.value)

        dummy.node_text_mand = MyEnum.a
        node = dummy.mk_node()
        self.assertEqual(2, len(node))
        self.assertEqual(node[0].text, MyEnum.a.value)
        self.assertEqual(node[1].text, MyEnum.b.value)
        for value in (42, b'hello'):
            try:
                dummy.node_text_mand = value
            except ValueError:
                pass
            else:
                raise Exception(f'dummy.node_text_mand = {value} did not raise ValueError!')


    def test_ExtensionNodeProperty(self):
        # ToDo:implement
        pass

    def test_SubElementProperty(self):
        dummy = DummySubElement()
        self.assertEqual(DummySubElement.sub_elem_mand.get_actual_value(dummy), None)
        self.assertEqual(DummySubElement.sub_elem_opt.get_actual_value(dummy), None)
        self.assertEqual(dummy.sub_elem_mand, CodedValue('foo'))
        self.assertEqual(dummy.sub_elem_opt, None)

        node = dummy.mk_node()
        self.assertEqual(1, len(node))  # the empty optional node is not added, only the mandatory one
        self.assertEqual(node[0].tag, '{pref}sub_elem_mand')
        self.assertEqual(node[0].text, None)
        self.assertEqual(0, len(node[0].attrib))

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

    def test_SubElementWithSubElementListProperty(self):
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
