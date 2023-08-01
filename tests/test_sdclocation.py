import unittest

from sdc11073.location import SdcLocation
from sdc11073.wsdiscovery.service import Service
from sdc11073.xml_types.wsd_types import ScopesType

class TestSdcLocation(unittest.TestCase):
    scheme = SdcLocation.scheme  # 'sdc.ctxt.loc'
    default_root = SdcLocation.location_detail_root  # 'sdc.ctxt.loc.detail'
    scope_prefix = scheme + ':/' + default_root  # sdc.ctxt.loc:/sdc.ctxt.loc.detail'

    def test_scopeString(self):
        expected_scope_string = self.scope_prefix + '/HOSP1%2F%2F%2FCU1%2F%2FBedA500?fac=HOSP1&poc=CU1&bed=BedA500'
        loc = SdcLocation(fac='HOSP1', poc='CU1', bed='BedA500')
        self.assertEqual(loc.root, self.default_root)
        self.assertEqual(loc.fac, 'HOSP1')
        self.assertEqual(loc.poc, 'CU1')
        self.assertEqual(loc.bed, 'BedA500')
        self.assertEqual(loc.rm, None)
        self.assertEqual(loc.bldng, None)
        self.assertEqual(loc.flr, None)
        self.assertEqual(loc.scope_string, expected_scope_string)

        # this is an unusual scope with bed only plus root
        expected_scope_string = self.scheme + ':/myroot/%2F%2F%2F%2F%2FBedA500?bed=BedA500'
        loc = SdcLocation(bed='BedA500', root='myroot')
        self.assertEqual(loc.root, 'myroot')
        self.assertEqual(loc.fac, None)
        self.assertEqual(loc.poc, None)
        self.assertEqual(loc.bed, 'BedA500')
        self.assertEqual(loc.rm, None)
        self.assertEqual(loc.bldng, None)
        self.assertEqual(loc.flr, None)
        self.assertEqual(loc.scope_string, expected_scope_string)

        # this is an unusual scope with all parameters and spaces in them
        loc = SdcLocation(fac='HOSP 1', poc='CU 1', bed='Bed A500', flr='flr 1', rm='rM 1', bldng='abc 1',
                          root='some where')
        self.assertEqual(loc.root, 'some where')
        self.assertEqual(loc.fac, 'HOSP 1')
        self.assertEqual(loc.poc, 'CU 1')
        self.assertEqual(loc.bed, 'Bed A500')
        self.assertEqual(loc.rm, 'rM 1')
        self.assertEqual(loc.bldng, 'abc 1')
        self.assertEqual(loc.flr, 'flr 1')

        self.assertEqual(loc, SdcLocation.from_scope_string(loc.scope_string))

    def test_fromScopeString(self):
        scope_string_sdc = self.scope_prefix + '/HOSP1%2F%2F%2FCU1%2F%2FBedA500?fac=HOSP1&poc=CU1&bed=BedA500'
        loc = SdcLocation.from_scope_string(scope_string_sdc)
        self.assertEqual(loc.root, self.default_root)
        self.assertEqual(loc.fac, 'HOSP1')
        self.assertEqual(loc.poc, 'CU1')
        self.assertEqual(loc.bed, 'BedA500')
        self.assertEqual(loc.rm, None)
        self.assertEqual(loc.bldng, None)
        self.assertEqual(loc.flr, None)
        self.assertEqual(loc.scope_string, scope_string_sdc)

        # correct handling of scope with %20 spaces and + char in query
        scopeString = self.scheme + ':/some%20where/HOSP%201%2Fabc%201%2FCU%201%2Fflr%201%2FrM%201%2FBed%20A500?rm=rM+1&flr=flr+1&bed=Bed+A500&bldng=abc+1&fac=HOSP+1&poc=CU+1'
        loc = SdcLocation.from_scope_string(scopeString)
        self.assertEqual(loc.root, 'some where')
        self.assertEqual(loc.fac, 'HOSP 1')
        self.assertEqual(loc.poc, 'CU 1')
        self.assertEqual(loc.bed, 'Bed A500')
        self.assertEqual(loc.rm, 'rM 1')
        self.assertEqual(loc.bldng, 'abc 1')
        self.assertEqual(loc.flr, 'flr 1')

        # if we can create another identical  DraegerLocation from loc, then scopeString also seems okay.
        self.assertEqual(loc, SdcLocation.from_scope_string(loc.scope_string))

        # correct handling of scope with %20 spaces also in query
        for scopeString in (
        self.scheme + ':/some%20where/HOSP%201%2Fabc%201%2FCU%201%2Fflr%201%2FrM%201%2FBed%20A500?rm=rM%201&flr=flr%201&bed=Bed+A500&bldng=abc+1&fac=HOSP+1&poc=CU+1',
        self.scheme + ':/some%20where/this_part_of string_does_not_matter?rm=rM%201&flr=flr%201&bed=Bed+A500&bldng=abc+1&fac=HOSP+1&poc=CU+1'):
            loc = SdcLocation.from_scope_string(scopeString)
            self.assertEqual(loc.root, 'some where')
            self.assertEqual(loc.fac, 'HOSP 1')
            self.assertEqual(loc.poc, 'CU 1')
            self.assertEqual(loc.bed, 'Bed A500')
            self.assertEqual(loc.rm, 'rM 1')
            self.assertEqual(loc.bldng, 'abc 1')
            self.assertEqual(loc.flr, 'flr 1')

    def test_equal(self):
        loc1 = SdcLocation(bed='BedA500', root='myroot')
        loc2 = SdcLocation(bed='BedA500', root='myroot')
        self.assertEqual(loc1, loc2)
        for attrName in ('root', 'fac', 'bldng', 'poc', 'flr', 'rm', 'bed'):
            print('different {} expected'.format(attrName))
            setattr(loc1, attrName, 'x')
            setattr(loc2, attrName, 'y')
            self.assertNotEqual(loc1, loc2)
            print('equal {} expected'.format(attrName))
            setattr(loc2, attrName, 'x')
            self.assertEqual(loc1, loc2)

    def test_contains(self):
        whole_world = SdcLocation()
        my_bed = SdcLocation(fac='fac1', poc='poc1', bed='bed1', bldng='bld1', flr='flr1', rm='rm1')
        my_bld = SdcLocation(fac='fac1', poc='poc1', bldng='bld1')
        other_bld = SdcLocation(fac='fac1', poc='poc1', bldng='bld2')
        any_flr1 = SdcLocation(flr='flr1')  # any location that has flr1 will match
        self.assertTrue(my_bed in whole_world)
        self.assertFalse(whole_world in my_bed)
        self.assertTrue(my_bed in SdcLocation(fac='fac1'))
        self.assertTrue(my_bed in SdcLocation(fac='fac1', poc='poc1'))
        self.assertTrue(my_bed in SdcLocation(fac='fac1', bed='bed1'))
        self.assertTrue(my_bed in SdcLocation(bed='bed1'))
        self.assertTrue(my_bed in my_bld)
        self.assertFalse(my_bld in my_bed)
        self.assertTrue(my_bed in any_flr1)
        self.assertFalse(my_bld in any_flr1)
        self.assertFalse(my_bed in other_bld)

        # non-default root
        my_bed = SdcLocation(fac='fac1', poc='poc1', bed='bed1', bldng='bld1', flr='flr1', rm='rm1', root='myroot')
        self.assertTrue(
            my_bed in SdcLocation(fac='fac1', poc='poc1', bed='bed1', bldng='bld1', flr='flr1', rm='rm1', root='myroot'))
        self.assertTrue(my_bed in SdcLocation(fac='fac1', root='myroot'))
        self.assertFalse(my_bed in SdcLocation(fac='fac2', root='myroot'))
        self.assertFalse(my_bed in SdcLocation(fac='fac1'))

    def test_filter_services_inside(self):
        my_loc = SdcLocation(fac='fac1', poc='poc1', bed='bed1', bldng='bld1', flr='flr1', rm='rm1', root='myroot')
        other_loc = SdcLocation(fac='fac2', poc='poc1', bed='bed1', bldng='bld1', flr='flr1', rm='rm1', root='myroot')
        service1 = Service(types=None, scopes=ScopesType(my_loc.scope_string), epr='a', x_addrs=None, instance_id='42')
        service2 = Service(types=None, scopes=ScopesType(other_loc.scope_string), epr='b', x_addrs=None, instance_id='42')
        service3 = Service(types=None, scopes=None, epr='b', x_addrs=None, instance_id='42')
        matches = my_loc.filter_services_inside((service1, service2, service3))
        self.assertEqual(len(matches), 1)
