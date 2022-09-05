import unittest

from sdc11073 import pmtypes


class TestPmtypes(unittest.TestCase):

    def test_CodedValue(self):
        c1 = pmtypes.CodedValue('42')
        c2 = pmtypes.CodedValue('42', coding_system='abc')
        # compare with simple string or int shall return False
        self.assertFalse(c1.equals(42))
        self.assertFalse(c1.equals('42'))
        # if CodedValue does not have default coding systen, this compare shall return False
        self.assertFalse(c2.equals(42))
        # it shall be possible to compare with a Coding instance
        self.assertTrue(c1.equals(pmtypes.Coding('42', pmtypes.DEFAULT_CODING_SYSTEM, None)))

        # if two CodedValue instances are compared, the translations shall also be handled
        c2.Translation.append(pmtypes.T_Translation('41'))
        self.assertNotEqual(c2, 41)
        c3 = pmtypes.CodedValue('42')
        c3.Translation.append(pmtypes.T_Translation('41')) # same translation as c2
        self.assertTrue(c2.equals(c3))

    def test_have_matching_codes(self):
        c1 = pmtypes.CodedValue('42', coding_system='abc')
        c1.Translation.append(pmtypes.T_Translation('41'))
        self.assertTrue(pmtypes.have_matching_codes(c1, pmtypes.Coding('42', coding_system='abc')))
        self.assertTrue(pmtypes.have_matching_codes(c1, pmtypes.Coding('41')))
        self.assertFalse(pmtypes.have_matching_codes(c1, pmtypes.Coding('41', coding_system='abc')))

        c2 = pmtypes.CodedValue('xxx', coding_system='abc')
        c2.Translation.append(pmtypes.T_Translation('41'))
        self.assertTrue(pmtypes.have_matching_codes(c1, c2))
