from __future__ import absolute_import
from __future__ import print_function 
import unittest
from sdc11073 import codings


class TestCodings(unittest.TestCase):
    def test_units(self):
        # test a unit that has entries for all three fields
        unit_lookup = codings.readStdUnits()  
        unitCoding = unit_lookup.code.getOne('268306')
        self.assertEqual(unitCoding.refId, 'MDC_DIM_CM_H2O_PER_MILLI_L')
        self.assertEqual(unitCoding.text, 'cmH2O/mL')        
        
        # test well known Foot unit  
        unitCoding = unit_lookup.refId.getOne('MDC_DIM_FOOT')
        self.assertEqual(unitCoding.refId, 'MDC_DIM_FOOT')
        self.assertEqual(unitCoding.code, '263488')
        self.assertEqual(unitCoding.text, 'ft')
        
        
def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestCodings)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())

