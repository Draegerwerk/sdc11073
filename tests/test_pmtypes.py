import unittest

from sdc11073 import pmtypes


class TestPmtypes(unittest.TestCase):

    def test_CodedValue(self):
        c1 = pmtypes.CodedValue(42)
        c2 = pmtypes.CodedValue(42, codingsystem='abc')
        # compare with simple string or int shall imply default coding system
        self.assertEqual(c1, 42)
        self.assertEqual(c1, '42')
        # if CodedValue does not have default coding systen, this compare shall return False
        self.assertNotEqual(c2, 42)
        # it shall be possible to compare with a Coding instance
        self.assertEqual(c1, pmtypes.Coding('42', pmtypes.DefaultCodingSystem, None))

        # if two CodedValue instances are compared, the translations shall also be handled
        c2.Translation.append(pmtypes.T_Translation(41))
        self.assertEqual(c2, 41)
        c3 = pmtypes.CodedValue(42)
        c3.Translation.append(pmtypes.T_Translation(41)) # same translation as c2
        self.assertEqual(c2, c3)

    def test_base_demographics(self):
        """Verify that Middlename is instantiated correctly as a list of strings"""
        bd = pmtypes.BaseDemographics()
        self.assertEqual(bd.Middlename, [])
        bd = pmtypes.BaseDemographics(middlenames='foo')
        self.assertEqual(bd.Middlename, ['foo'])
        bd = pmtypes.BaseDemographics(middlenames=['foo', 'bar'])
        self.assertEqual(bd.Middlename, ['foo', 'bar'])
