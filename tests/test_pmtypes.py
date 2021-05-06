import unittest

from sdc11073 import pmtypes


class TestPmtypes(unittest.TestCase):

    def test_CodedValue(self):
        c1 = pmtypes.CodedValue(42)
        c2 = pmtypes.CodedValue(42, codingsystem='abc')
        # compare with simple string or int shall imply default coding system
        self.assertTrue(c1.equals(42))
        self.assertTrue(c1.equals('42'))
        # if CodedValue does not have default coding systen, this compare shall return False
        self.assertFalse(c2.equals(42))
        # it shall be possible to compare with a Coding instance
        self.assertTrue(c1.equals(pmtypes.Coding('42', pmtypes.DefaultCodingSystem, None)))

        # if two CodedValue instances are compared, the translations shall also be handled
        c2.Translation.append(pmtypes.T_Translation(41))
        self.assertNotEqual(c2, 41)
        c3 = pmtypes.CodedValue(42)
        c3.Translation.append(pmtypes.T_Translation(41)) # same translation as c2
        self.assertTrue(c2.equals(c3))
