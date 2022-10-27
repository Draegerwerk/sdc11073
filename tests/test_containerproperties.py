import datetime
import unittest

from sdc11073.isoduration import UTC
from sdc11073.mdib.containerproperties import DateOfBirthProperty as DoB


# pylint: disable=protected-access


class TestContainerProperties(unittest.TestCase):

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
