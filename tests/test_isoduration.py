import unittest
from sdc11073.isoduration import parse_duration, durationString
from sdc11073.isoduration import parse_date_time, date_time_string, GYearMonth, GYear, UTC
from datetime import date, datetime

class TestIsoDate(unittest.TestCase):
    
    def test_durationString(self):
        self.assertEqual('PT0H0M1S', durationString(1))
        self.assertEqual('PT0H1M0S', durationString(60))
        self.assertEqual('PT0H1M1S', durationString(61))
        self.assertEqual('P0Y0M3DT5H13M17S', durationString(3600*24*3 +3600*5 + 60*13 +17))
        self.assertEqual('-P0Y0M3DT5H13M17S', durationString((3600*24*3 +3600*5 + 60*13 +17)*-1))
        self.assertEqual('PT0H0M0.1S', durationString(0.1))
        self.assertEqual('-PT0H0M0.000001S', durationString(-0.000001))


    def test_parseDuration(self):
        self.assertEqual(parse_duration('P0Y0M0DT0H0M1S'), 1)
        self.assertEqual(parse_duration('P0Y0M0DT0H1M0S'), 60)
        self.assertEqual(parse_duration('P0Y0M0DT0H1M1S'), 61)
        self.assertEqual(parse_duration('P0Y0M0DT0H1M61S'), 121)
        self.assertEqual(parse_duration('P0Y0M3DT5H13M17S'), 3600*24*3 +3600*5 + 60*13 +17)
        self.assertEqual(parse_duration('-P0Y0M3DT5H13M17S'), (3600*24*3 +3600*5 + 60*13 +17)*-1)
        self.assertEqual(parse_duration('P0Y0M0DT0H0M0.1S'), 0.1)
        # some shorter representations:
        self.assertEqual(parse_duration('PT0H0M1S'), 1)
        self.assertEqual(parse_duration('PT1S'), 1)
        self.assertEqual(parse_duration('PT1M'), 60)
        self.assertEqual(parse_duration('P0DT1M1S'), 61)
        self.assertEqual(parse_duration('P3DT5H13M17S'), 3600*24*3 +3600*5 + 60*13 +17)
        self.assertEqual(parse_duration('P3D'), 3600*24*3)
        self.assertEqual(parse_duration('-PT00H00M00.000001000S'), -0.000001)

    def test_parse_date_time(self):
        self.assertEqual(parse_date_time('2015-05-25'), date(2015, 5, 25))
        self.assertEqual(parse_date_time('20150525'), date(2015, 5, 25))
        self.assertEqual(parse_date_time('2015-05-25T14:45:00'), datetime(2015, 5, 25, 14, 45, 00))
        self.assertEqual(parse_date_time('2015-05-25 14:45:00', strict=False), datetime(2015, 5, 25, 14, 45, 00))
        self.assertEqual(parse_date_time('2015-05-25 14:45:00'), date(2015, 5, 25))
        result = parse_date_time('2015-05-25T14:45:00+01:00')
        self.assertEqual(result.hour, 14)
        self.assertEqual(result.tzinfo.utcoffset(0).seconds, 3600)
        self.assertEqual(parse_date_time('2015-05'), GYearMonth(2015, 5))
        self.assertEqual(parse_date_time('2015'), GYear(2015))

    def test_date_time_string(self):
        self.assertEqual(date_time_string(date(2015, 5, 25)), '2015-05-25')
        self.assertEqual(date_time_string(datetime(2015, 5, 25, 14, 45, 00)), '2015-05-25T14:45:00')
        self.assertEqual(date_time_string(datetime(2015, 5, 25, 14, 45, 00, tzinfo=UTC(60))), '2015-05-25T14:45:00+01:00')
        self.assertEqual(date_time_string(datetime(2015, 5, 25, 14, 45, 00, tzinfo=UTC(-60))), '2015-05-25T14:45:00-01:00')
        self.assertEqual(date_time_string(GYearMonth(2015, 5)), '2015-05')
        self.assertEqual(date_time_string(GYear(2015)), '2015')

        

def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestIsoDate)


if __name__ == '__main__':
    unittest.TextTestRunner(verbosity=2).run(suite())
        