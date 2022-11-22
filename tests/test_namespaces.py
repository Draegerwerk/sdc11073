import unittest

from sdc11073 import namespaces


class TestNamespaces(unittest.TestCase):

    def test_default(self):
        hlp = namespaces.NamespaceHelper(namespaces.PrefixesEnum)
        self.assertEqual(hlp.MSG.prefix, 'msg')

        bla_tag = hlp.MSG.tag('bla')
        self.assertEqual('bla', bla_tag.localname)
        self.assertEqual(namespaces.PrefixesEnum.MSG.namespace, bla_tag.namespace)

        bla_string = hlp.doc_name_from_qname(bla_tag)
        self.assertEqual('msg:bla', bla_string)

    def test_default_ns(self):
        hlp = namespaces.NamespaceHelper(namespaces.PrefixesEnum, default_ns_key='MSG')
        self.assertEqual(hlp.MSG.prefix, 'msg')

        bla_tag = hlp.msgTag('bla')
        self.assertEqual('bla', bla_tag.localname)
        self.assertEqual(namespaces.PrefixesEnum.MSG.namespace, bla_tag.namespace)

        bla_string = hlp.doc_name_from_qname(bla_tag)
        self.assertEqual('bla', bla_string)

    def test_mapped(self):
        ns_map = {
            'abc': namespaces.PrefixesEnum.MSG.namespace
        }
        hlp = namespaces.NamespaceHelper(namespaces.PrefixesEnum, ns_map)

        bla_tag = hlp.MSG.tag('bla')
        bla_string = hlp.doc_name_from_qname(bla_tag)
        self.assertEqual('abc:bla', bla_string)
