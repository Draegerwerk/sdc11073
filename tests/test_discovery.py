import threading
import typing
import unittest
import uuid
from unittest import mock
from sdc11073 import wsdiscovery
import logging
import time
import urllib
import socket

from sdc11073 import loghelper #, definitions_sdc, location
from tests import utils

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
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sh.setFormatter(formatter)
    wsdlog.addHandler(sh)

    srvlog = logging.getLogger("wsdService")
    srvlog.setLevel(logging.DEBUG)
    # create console handler and set level to debug
    sh = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sh.setFormatter(formatter)
    srvlog.addHandler(sh)

    testlog = logging.getLogger("unittest")
    testlog.setLevel(logging.INFO)
    # create console handler and set level to debug
    sh = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    sh.setFormatter(formatter)
    testlog.addHandler(sh)
    

class TestDiscovery(unittest.TestCase):
    SEARCH_TIMEOUT = 2
    MY_MULTICAST_PORT = 37020  # change port, otherwise windows steals unicast messages

    def setUp(self):
        testlog.debug('setUp {}'.format(self._testMethodName))
        # give them different logger names so that output can be distinguished
        self.wsdclient = wsdiscovery.WSDiscoveryWhitelist(acceptedAdapterIPAddresses=['127.0.0.1'], 
                                                          logger=loghelper.getLoggerAdapter('wsdclient'),
                                                          multicast_port=self.MY_MULTICAST_PORT)
        self.wsdService = wsdiscovery.WSDiscoveryWhitelist(acceptedAdapterIPAddresses=['127.0.0.1'],
                                                           logger=loghelper.getLoggerAdapter('wsdService'),
                                                           multicast_port=self.MY_MULTICAST_PORT)
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
        
        ttype1 = utils.random_qname()
        scope1 = utils.random_scope()
        ttype2 = utils.random_qname()
        scope2 = utils.random_scope()
        
        xAddrs = [f"localhost:8080/{uuid.uuid4()}", '{ip}/' + str(uuid.uuid4())]
        epr = uuid.uuid4().hex
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
        
        ttype = utils.random_qname()
        scope = utils.random_scope()
        
        xAddrs = [f"localhost:8080/{uuid.uuid4()}", ]
        epr = uuid.uuid4().hex
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
        """ if a service has no epr in ProbeMatches response, it shall be ignored."""
        self.wsdService.PROBEMATCH_EPR = False
        testlog.info('starting service...')
        self.wsdService.start()

        ttype = utils.random_qname()
        scope = utils.random_scope()

        xAddrs = [f"localhost:8080/{uuid.uuid4()}", ]
        epr = uuid.uuid4().hex
        self.wsdService.publishService(epr, types=[ttype], scopes=[scope], xAddrs=xAddrs)
        time.sleep(5) # make sure hello messages are all sent before client discovery starts

        testlog.info('starting client...')
        self.wsdclient.start()
        time.sleep(0.2)
        testlog.info('starting search...')
        self.wsdclient.clearRemoteServices()
        services = self.wsdclient.searchServices(timeout=self.SEARCH_TIMEOUT)
        testlog.info('search done.')
        self.assertEqual(len([s for s in services if s.getEPR() == epr]), 0)


    def test_discover_noTYPES(self):
        """ if a service has no types in ProbeMatches response, the client shall send a resolve and add that result."""
        self.wsdService.PROBEMATCH_TYPES = False
        testlog.info('starting service...')
        self.wsdService.start()

        ttype = utils.random_qname()
        scope = utils.random_scope()

        xAddrs = [f"localhost:8080/{uuid.uuid4()}", ]
        epr = uuid.uuid4().hex
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
        """ if a service has no scopes in ProbeMatches response, the client shall send a resolve and add that result."""
        self.wsdService.PROBEMATCH_SCOPES = False
        testlog.info('starting service...')
        self.wsdService.start()

        ttype = utils.random_qname()
        scope = utils.random_scope()

        xAddrs = [f"localhost:8080/{uuid.uuid4()}", ]
        epr = uuid.uuid4().hex
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
        """ if a service has no x-addresses in ProbeMatches response, the client shall send a resolve and add that result."""
        self.wsdService.PROBEMATCH_XADDRS = False
        testlog.info('starting service...')
        self.wsdService.start()

        ttype = utils.random_qname()
        scope = utils.random_scope()

        xAddrs = [f"localhost:8080/{uuid.uuid4()}", ]
        epr = uuid.uuid4().hex
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
        device_count = 20
        eprs = [uuid.uuid4().hex for _ in range(device_count)]
        for i, epr in enumerate(eprs):
            ttype1 = utils.random_qname()
            scope1 = utils.random_scope()
            xAddrs = ["localhost:{}/{}".format(8080+i, epr)]
            self.wsdService.publishService(epr, types=[ttype1], scopes=[scope1], xAddrs=xAddrs)
            
        time.sleep(3.02)
        testlog.info('starting client...') 
        self.wsdclient.start()
        services = self.wsdclient.searchServices(timeout=3)
        testlog.info('search done.') 
        
        for service in services:
            testlog.info('found service: {} : {}'.format(service.getEPR(), service.getXAddrs()))
        myServices = [s for s in services if s.getEPR() in eprs] # there might be other devices in the network
        self.assertEqual(len(myServices), device_count)


    def test_publishManyServices_earlyStartedClient(self):
        # verify if client keeps track of all devices that appeared / disappeared after start without searching. 
        testlog.info('starting client...') 
        self.wsdclient.start()
        time.sleep(0.01)
        testlog.info('starting service...') 
        self.wsdService.start()
        device_count = 20
        eprs = [uuid.uuid4().hex for _ in range(device_count)]
        epr_event_map: typing.Dict[str, threading.Event] = {epr: threading.Event() for epr in eprs}
        for i, epr in enumerate(eprs):
            ttype1 = utils.random_qname()
            scope1 = utils.random_scope()
            xAddrs = ["localhost:{}/{}".format(8080 + i, epr)]
            self.wsdService.publishService(epr, types=[ttype1], scopes=[scope1], xAddrs=xAddrs)
            
        time.sleep(2.02)
        self.assertEqual(len([epr for epr in self.wsdclient._remoteServices if epr in eprs]), device_count)
        testlog.info('stopping service...')
        def _remote_service_bye(_: str, epr_: str) -> None:
            if epr_ in eprs:
                self.assertTrue(epr_ not in self.wsdclient._remoteServices)
                epr_event_map[epr_].set()

        self.wsdclient.setRemoteServiceByeCallback(_remote_service_bye)
        self.wsdService.clearLocalServices()
        for event in epr_event_map.values():
            self.assertTrue(event.wait(timeout=5.0))

    def test_unexpected_multicast_messages(self):
        """verify that module is robust against all kind of invalid multicast and single cast messages"""

        address = '127.0.0.1'
        unicast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        def send_and_assert_running(data):
            unicast_sock.sendto(data.encode('utf-8'), (address, self.MY_MULTICAST_PORT))
            time.sleep(0.1)
            self.assertTrue(self.wsdService._networkingThread._recvThread.is_alive())
            self.assertTrue(self.wsdService._networkingThread._qread_thread.is_alive())
            self.assertTrue(self.wsdService._networkingThread._sendThread.is_alive())
            self.assertTrue(self.wsdService._addrsMonitorThread.is_alive())

        self.wsdService.start()
        time.sleep(0.1)

        try:
            send_and_assert_running('no xml at all')
            send_and_assert_running(f'<bla>invalid xml fragment</bla>')
        finally:
            unicast_sock.close()

    @unittest.skip
    def test_multicast_listening(self):
        """verify that module only listens on accepted ports"""
        # TODO: why does this test fail often on github?
        testlog.info('starting service...')
        wsd_service_all = wsdiscovery.WSDiscoveryBlacklist(logger=loghelper.getLoggerAdapter('wsdService'),
                                                           multicast_port=self.MY_MULTICAST_PORT)
        wsd_service_all.start()
        time.sleep(0.1)
        all_addresses = wsdiscovery.get_ipv4_addresses()
        unicast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            # wsd_service_all listens on all ports, all udp uni cast messages shall be handled
            obj = wsd_service_all._networkingThread
            with mock.patch.object(obj, '_add_to_recv_queue', wraps=obj._add_to_recv_queue) as wrapped_obj:
                for address in all_addresses:
                    unicast_sock.sendto(f'<bla>unicast{address} all </bla>'.encode('utf-8'),
                                        (address, self.MY_MULTICAST_PORT))
                time.sleep(0.5)
                self.assertGreaterEqual(wrapped_obj.call_count, len(all_addresses))

            wsd_service_all.stop()  # do not interfere with next instance

            # self.wsd_service  listens only on localhost, udp messages to other adapters shall not be handled
            self.wsdService.start()
            time.sleep(0.1)
            obj = self.wsdService._networkingThread
            with mock.patch.object(obj, '_add_to_recv_queue', wraps=obj._add_to_recv_queue) as wrapped_obj:
                for address in all_addresses:
                    unicast_sock.sendto(f'<bla>unicast{address} all </bla>'.encode('utf-8'),
                                        (address, self.MY_MULTICAST_PORT))
                time.sleep(0.5)
                wrapped_obj.assert_called()
        finally:
            unicast_sock.close()


def suite():
    return unittest.TestLoader().loadTestsFromTestCase(TestDiscovery)


if __name__ == '__main__':
    logger = logging.getLogger('sdc')
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    logger.addHandler(ch)
     
    logger.setLevel(logging.DEBUG)

    unittest.TextTestRunner(verbosity=2).run(unittest.TestLoader().loadTestsFromName('testdiscovery.TestDiscovery.test_publishManyServices_earlyStartedClient'))
