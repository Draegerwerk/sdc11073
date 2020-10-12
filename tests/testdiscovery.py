import unittest
from sdc11073 import wsdiscovery
import logging
import time
import urllib
from sdc11073 import loghelper #, definitions_sdc, location

QName = wsdiscovery.QName
Scope = wsdiscovery.Scope

testlog = None

#pylint: disable=protected-access

def setUpModule():
    global testlog
    wsdlog = logging.getLogger("wsdclient")
    wsdlog.setLevel(logging.DEBUG)
    # create console handler and set level to debug
    sh = logging.StreamHandler()
#    sh.setLevel(logging.DEBUG)
    # create formatter
#    formatter = logging.Formatter("******************************\n%(asctime)s - %(name)s - %(levelname)s - %(message)s\n******************************")
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to ch
    sh.setFormatter(formatter)
    # add ch to logger
    wsdlog.addHandler(sh)

    srvlog = logging.getLogger("wsdService")
    srvlog.setLevel(logging.DEBUG)
    # create console handler and set level to debug
    sh = logging.StreamHandler()
#    sh.setLevel(logging.DEBUG)
    # create formatter
#    formatter = logging.Formatter("-------------------------------------\n%(asctime)s - %(name)s - %(levelname)s - %(message)s\n-------------------------------------")
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to sh
    sh.setFormatter(formatter)
    # add ch to logger
    srvlog.addHandler(sh)

    testlog = logging.getLogger("unittest")
    testlog.setLevel(logging.INFO)
    # create console handler and set level to debug
    sh = logging.StreamHandler()
#    sh.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to ch
    sh.setFormatter(formatter)
    # add ch to logger
    testlog.addHandler(sh)
    

class TestDiscovery(unittest.TestCase):
    SEARCH_TIMEOUT = 2
    def setUp(self):
        testlog.debug('setUp {}'.format(self._testMethodName))
        # give them different logger names so that output can be distinguished
        self.wsdclient = wsdiscovery.WSDiscoveryWhitelist(acceptedAdapterIPAddresses=['127.0.0.1'], 
                                                          logger=loghelper.getLoggerAdapter('wsdclient'))
        self.wsdService = wsdiscovery.WSDiscoveryWhitelist(acceptedAdapterIPAddresses=['127.0.0.1'],
                                                           logger=loghelper.getLoggerAdapter('wsdService'))
        testlog.debug('setUp done{}'.format(self._testMethodName))


    def tearDown(self):
        testlog.debug('tearDown {}'.format(self._testMethodName))
        self.wsdclient.stop()
        self.wsdService.stop()
        testlog.debug('tearDown done {}'.format(self._testMethodName))

    def test_discover(self):
        testlog.info('starting client...')
        self.wsdclient.start()
        time.sleep(0.1)
        testlog.info('starting service...') 
        self.wsdService.start()
        
        ttype1 = QName("abc", "def")
        scope1 = Scope("http://myscope")
        ttype2 = QName("namespace", "myOtherTestService_type1")
        scope2 = Scope("http://other_scope")
        
        xAddrs = ["localhost:8080/abc", '{ip}/device_service']
        epr = 'my_epr'
        self.wsdService.publishService(epr, types=[ttype1], scopes=[scope1], xAddrs=xAddrs)
        time.sleep(0.2)
        
        # test that unfiltered search delivers at least my service
        testlog.info('starting search no filter...') 
        services = self.wsdclient.searchServices(timeout=self.SEARCH_TIMEOUT)
        myServices = [s for s in services if s.getEPR() == epr]
        self.assertEqual(len(myServices), 1)

        # test that filtered search (types) delivers only my service
        testlog.info('starting search types filter...') 
        services = self.wsdclient.searchServices(types=[ttype1], timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)
        myServices = [s for s in services if s.getEPR() == epr]
        self.assertEqual(len(myServices), 1)

        # test that filtered search (scopes) delivers only my service
        testlog.info('starting search scopes filter...') 
        services = self.wsdclient.searchServices(scopes=[scope1], timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)
        myServices = [s for s in services if s.getEPR() == epr]
        self.assertEqual(len(myServices), 1)

        # test that filtered search (scopes+types) delivers only my service
        testlog.info('starting search scopes+types filter...') 
        services = self.wsdclient.searchServices(types=[ttype1], scopes=[scope1], timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)
        myServices = [s for s in services if s.getEPR() == epr]
        self.assertEqual(len(myServices), 1)

        # test that filtered search (wrong type) finds no service
        testlog.info('starting search types filter...') 
        services = self.wsdclient.searchServices(types=[ttype2], timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 0)

        # test that filtered search (wrong scope) finds no service
        testlog.info('starting search wrong scopes filter...') 
        services = self.wsdclient.searchServices(scopes=[scope2], timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 0)

        # test that filtered search (correct scopes+ wrong types) finds no service
        testlog.info('starting search scopes+types filter...') 
        services = self.wsdclient.searchServices(types=[ttype2], scopes=[scope1], timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 0)

        # test that filtered search (wrong scopes + wrong types) finds no service
        testlog.info('starting search scopes+types filter...') 
        services = self.wsdclient.searchServices(types=[ttype1], scopes=[scope2], timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 0)


    def test_discover_serviceFirst(self):
        testlog.info('starting service...') 
        self.wsdService.start()
        
        ttype = QName("abc", "def")
        scope = Scope("http://other_scope")
        
        xAddrs = ["localhost:8080/abc", ]
        epr = 'my_epr'
        self.wsdService.publishService(epr, types=[ttype], scopes=[scope], xAddrs=xAddrs)
        time.sleep(2)
        
        testlog.info('starting client...') 
        self.wsdclient.start()
        time.sleep(0.2)
        testlog.info('starting search...') 
        services = self.wsdclient.searchServices(timeout=self.SEARCH_TIMEOUT)
        testlog.info('search done.') 
        
        for service in services:
            testlog.info('found service: {} : {}'.format(service.getEPR(), service.getXAddrs()))
        myServices = [s for s in services if s.getEPR() == epr]
        self.assertEqual(len(myServices), 1)


    def test_discover_noEPR(self):
        ''' if a service has no epr in ProbeMatches response, it shall be ignored.'''
        self.wsdService.PROBEMATCH_EPR = False
        testlog.info('starting service...')
        self.wsdService.start()

        ttype = QName("abc", "def")
        scope = Scope("http://other_scope")

        xAddrs = ["localhost:8080/abc", ]
        epr = 'my_epr'
        self.wsdService.publishService(epr, types=[ttype], scopes=[scope], xAddrs=xAddrs)
        time.sleep(2)

        testlog.info('starting client...')
        self.wsdclient.start()
        time.sleep(0.2)
        testlog.info('starting search...')
        services = self.wsdclient.searchServices(timeout=self.SEARCH_TIMEOUT)
        testlog.info('search done.')
        self.assertEqual(len(services), 0)


    def test_discover_noTYPES(self):
        ''' if a service has no types in ProbeMatches response, the client shall send a resolve and add that result.'''
        self.wsdService.PROBEMATCH_TYPES = False
        testlog.info('starting service...')
        self.wsdService.start()

        ttype = QName("abc", "def")
        scope = Scope("http://other_scope")

        xAddrs = ["localhost:8080/abc", ]
        epr = 'my_epr'
        self.wsdService.publishService(epr, types=[ttype], scopes=[scope], xAddrs=xAddrs)
        time.sleep(2)

        testlog.info('starting client...')
        self.wsdclient.start()
        time.sleep(0.2)
        testlog.info('starting search...')
        services = self.wsdclient.searchServices(timeout=self.SEARCH_TIMEOUT)
        testlog.info('search done.')

        for service in services:
            testlog.info('found service: {} : {}'.format(service.getEPR(), service.getXAddrs()))
        myServices = [s for s in services if s.getEPR() == epr]
        self.assertEqual(len(myServices), 1)
        self.assertEqual(myServices[0].getTypes(), [ttype])

    def test_discover_noScopes(self):
        ''' if a service has no scopes in ProbeMatches response, the client shall send a resolve and add that result.'''
        self.wsdService.PROBEMATCH_SCOPES = False
        testlog.info('starting service...')
        self.wsdService.start()

        ttype = QName("abc", "def")
        scope = Scope("http://other_scope")

        xAddrs = ["localhost:8080/abc", ]
        epr = 'my_epr'
        self.wsdService.publishService(epr, types=[ttype], scopes=[scope], xAddrs=xAddrs)
        time.sleep(2)

        testlog.info('starting client...')
        self.wsdclient.start()
        time.sleep(0.2)
        testlog.info('starting search...')
        services = self.wsdclient.searchServices(timeout=self.SEARCH_TIMEOUT)
        testlog.info('search done.')

        for service in services:
            testlog.info('found service: {} : {}'.format(service.getEPR(), service.getXAddrs()))
        myServices = [s for s in services if s.getEPR() == epr]
        self.assertEqual(len(myServices), 1)
        self.assertEqual(myServices[0].getTypes(), [ttype])

    def test_discover_noXaddrs(self):
        ''' if a service has no x-addresses in ProbeMatches response, the client shall send a resolve and add that result.'''
        self.wsdService.PROBEMATCH_XADDRS = False
        testlog.info('starting service...')
        self.wsdService.start()

        ttype = QName("abc", "def")
        scope = Scope("http://other_scope")

        xAddrs = ["localhost:8080/abc", ]
        epr = 'my_epr'
        self.wsdService.publishService(epr, types=[ttype], scopes=[scope], xAddrs=xAddrs)
        time.sleep(2)

        testlog.info('starting client...')
        self.wsdclient.start()
        time.sleep(0.2)
        testlog.info('starting search...')
        services = self.wsdclient.searchServices(timeout=self.SEARCH_TIMEOUT)
        testlog.info('search done.')

        for service in services:
            testlog.info('found service: {} : {}'.format(service.getEPR(), service.getXAddrs()))
        myServices = [s for s in services if s.getEPR() == epr]
        self.assertEqual(len(myServices), 1)
        self.assertEqual(myServices[0].getTypes(), [ttype])


    def test_ScopeMatch(self):
        myScope = 'biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2FBed?fac=HOSP1&bldng=b1&poc=CU1&flr=1&rm=r2&bed=Bed'
        otherScopes = [('biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2FBed?fac=HOSP1&bldng=b1&poc=CU1&flr=1&rm=r2&bed=Bed', True, 'identical'),
                       ('biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2FBed?fac=HOSP1&bldng=b1&poc=CU1&flr=1&rm=r2&bed=NoBed', True, 'different query'),
                       ('biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1/b1/CU1/1/r2/Bed?fac=HOSP1&bldng=b1&poc=CU1&flr=1&rm=r2&bed=NoBed', False, 'real slashes'),
                       ('biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2FBed', True, 'no query'),
                       ('biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2FBed/Longer', True, 'longer target'),
                       ('Biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2FBed?fac=HOSP1&bldng=b1&poc=CU1&flr=1&rm=r2&bed=Bed', True, 'different case schema'),
                       ('biceps.ctxt.location:/Biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2FBed?fac=HOSP1&bldng=b1&poc=CU1&flr=1&rm=r2&bed=Bed', False, 'different case root'),
                       ('biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2Fbed?fac=HOSP1&bldng=b1&poc=CU1&flr=1&rm=r2&bed=Bed', False, 'different case bed'),
                       ]
        matchBy = wsdiscovery.MATCH_BY_URI
        for otherScope, expectedmatchResult, remark in otherScopes:
            testlog.info('checking otherscope {}'.format(remark)) 
            parsed = urllib.parse.urlparse(otherScope)
            testlog.info('checking otherscope {}'.format(parsed))
            testlog.info('urlsplit {} = {}'.format(parsed.path, urllib.parse.urlsplit(parsed.path)))
            result = wsdiscovery.matchScope(myScope, otherScope, matchBy)
            self.assertEqual(expectedmatchResult, result, msg = remark)
        # Longer matches myScope, but not the other way round
        longer = otherScopes[4][0]
        self.assertTrue(wsdiscovery.matchScope(myScope, longer, matchBy), msg = 'short scope matches longer scope')
        self.assertFalse(wsdiscovery.matchScope(longer, myScope, matchBy), msg = 'long scope shall not match short scope')

        
    def test_publishManyServices_lateStartedClient(self):
        testlog.info('starting service...') 
        self.wsdService.start()
        deviceCount = 20
        for i in range(deviceCount):
            ttype1 = QName("namespace", "myOtherTestService_type1")
            scope1 = Scope("http://other_scope")
        
            epr = 'my_epr{}'.format(i)
            xAddrs = ["localhost:{}/{}".format(8080+i, epr)]
            self.wsdService.publishService(epr, types=[ttype1], scopes=[scope1], xAddrs=xAddrs)
            
        time.sleep(3.02)
        testlog.info('starting client...') 
        self.wsdclient.start()
        services = self.wsdclient.searchServices(timeout=1)
        testlog.info('search done.') 
        
        for service in services:
            testlog.info('found service: {} : {}'.format(service.getEPR(), service.getXAddrs()))
        myServices = [s for s in services if 'my_epr' in s.getEPR()] # there might be other devices in the network
        self.assertEqual(len(myServices), deviceCount)


    def test_publishManyServices_earlyStartedClient(self):
        # verify if client keeps track of all devices that appeared / disappeared after start without searching. 
        testlog.info('starting client...') 
        self.wsdclient.start()
        time.sleep(0.01)
        testlog.info('starting service...') 
        self.wsdService.start()
        deviceCount = 20
        for i in range(deviceCount):
            ttype1 = QName("namespace", "myOtherTestService_type1")
            scope1 = Scope("http://other_scope")
        
            epr = 'my_epr{}'.format(i)
            xAddrs = ["localhost:{}/{}".format(8080+i, epr)]
            self.wsdService.publishService(epr, types=[ttype1], scopes=[scope1], xAddrs=xAddrs)
            
        time.sleep(2.02)
        self.assertEqual(len(self.wsdclient._remoteServices), deviceCount)
        testlog.info('stopping service...') 
        self.wsdService.clearLocalServices()
        time.sleep(2.02)
        self.assertEqual(len(self.wsdclient._remoteServices), 0)

        
        
def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestDiscovery)


if __name__ == '__main__':
    logger = logging.getLogger('sdc')
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)
     
    logger.setLevel(logging.DEBUG)

    unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromName('testdiscovery.TestDiscovery.test_publishManyServices_earlyStartedClient'))
    
#    unittest.TextTestRunner(verbosity=2).run(suite())
