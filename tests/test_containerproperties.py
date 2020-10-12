import unittest
import datetime

import sdc11073.mdib.containerproperties as containerproperties

#pylint: disable=protected-access

DoB = containerproperties.DateOfBirthProperty
   
class TestContainerproperties(unittest.TestCase):

    def test_DateOfBirthRegEx(self):
        result = DoB.mk_value_object('2003-06-30')
        self.assertEqual(result, datetime.date(2003, 6, 30) )

        for text in ('foo', '0000-06-30', '01-00-01','01-01-00'): # several invalid strings
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
        self.assertEqual(result.tzinfo.utcoffset(0), datetime.timedelta(minutes=60*6+2))

        # add time zone -3hours
        result = DoB.mk_value_object('2003-06-30T15:53:12.4-03:01')
        self.assertEqual(result.second, 12)
        self.assertEqual(result.hour, 15)
        self.assertEqual(result.tzinfo.utcoffset(0), datetime.timedelta(minutes=(30*6+1)*-1))


    def test_DateOfBirth_toString(self):
        datestring = DoB._mk_datestring(datetime.date(2004, 3, 6))
        self.assertEqual(datestring, '2004-03-06')
        
        datestring = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16))
        self.assertEqual(datestring, '2004-03-06T14:15:16')

        datestring = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 4, 5, 6))
        self.assertEqual(datestring, '2004-03-06T04:05:06')# verify leading zeros in date and time  

        datestring = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000))
        self.assertEqual(datestring, '2004-03-06T14:15:16.7')

        datestring = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=containerproperties.UTC(0, 'UTC')))
        self.assertEqual(datestring, '2004-03-06T14:15:16.7Z')
        
        datestring = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=containerproperties.UTC(180, 'UTC+1')))
        self.assertEqual(datestring, '2004-03-06T14:15:16.7+03:00')

        datestring = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=containerproperties.UTC(-120, 'UTC+1')))
        self.assertEqual(datestring, '2004-03-06T14:15:16.7-02:00')
        
        datestring = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=containerproperties.UTC(181, 'UTC+1')))
        self.assertEqual(datestring, '2004-03-06T14:15:16.7+03:01')

        datestring = DoB._mk_datestring(datetime.datetime(2004, 3, 6, 14, 15, 16, 700000, tzinfo=containerproperties.UTC(-121, 'UTC+1')))
        self.assertEqual(datestring, '2004-03-06T14:15:16.7-02:01')
        
        
def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestContainerproperties)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
