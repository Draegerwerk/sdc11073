import logging
import socket
import time
import sys
import unittest
from urllib.parse import urlparse, urlsplit

from sdc11073 import loghelper
from sdc11073 import wsdiscovery
from sdc11073.xml_types.wsd_types import ScopesType

QName = wsdiscovery.QName

test_log = logging.getLogger("unittest")

_formatter_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


# pylint: disable=protected-access

def setUpModule():
    global test_log
    wsd_log = logging.getLogger("wsd_client")
    wsd_log.setLevel(logging.DEBUG)
    # create console handler and set level to debug
    sh = logging.StreamHandler()
    #    sh.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter(_formatter_string)
    # add formatter to ch
    sh.setFormatter(formatter)
    # add ch to logger
    wsd_log.addHandler(sh)

    srv_log = logging.getLogger("wsd_service")
    srv_log.setLevel(logging.DEBUG)
    # create console handler and set level to debug
    sh = logging.StreamHandler()
    #    sh.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter(_formatter_string)
    # add formatter to sh
    sh.setFormatter(formatter)
    # add ch to logger
    srv_log.addHandler(sh)

    # test_log = logging.getLogger("unittest")
    test_log.setLevel(logging.INFO)
    # create console handler and set level to debug
    sh = logging.StreamHandler()
    #    sh.setLevel(logging.DEBUG)
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to ch
    sh.setFormatter(formatter)
    # add ch to logger
    test_log.addHandler(sh)


class TestDiscovery(unittest.TestCase):
    SEARCH_TIMEOUT = 2

    def setUp(self):
        test_log.debug('setUp {}'.format(self._testMethodName))

        # give them different logger names so that output can be distinguished
        self.wsd_client = wsdiscovery.WSDiscoveryWhitelist(accepted_adapter_addresses=['127.0.0.1'],
                                                               logger=loghelper.get_logger_adapter('wsd_client'))
        self.wsd_service = wsdiscovery.WSDiscoveryWhitelist(accepted_adapter_addresses=['127.0.0.1'],
                                                                logger=loghelper.get_logger_adapter('wsd_service'))
        self.log_watcher_client = loghelper.LogWatcher(logging.getLogger('wsd_client'), level=logging.ERROR)
        self.log_watcher_service = loghelper.LogWatcher(logging.getLogger('wsd_service'), level=logging.ERROR)

        test_log.debug('setUp done{}'.format(self._testMethodName))

    def tearDown(self):
        test_log.debug('tearDown {}'.format(self._testMethodName))
        self.wsd_client.stop()
        self.wsd_service.stop()

        try:
            self.log_watcher_client.check()
        except loghelper.LogWatchException as ex:
            sys.stderr.write(repr(ex))
            raise
        try:
            self.log_watcher_service.check()
        except loghelper.LogWatchException as ex:
            sys.stderr.write(repr(ex))
            raise

        test_log.debug('tearDown done {}'.format(self._testMethodName))

    def test_discover(self):
        test_log.info('starting client...')
        test_log.info('starting service...')

        self.wsd_service.start()
        self.wsd_client.start()
        time.sleep(0.1)

        ttype1 = QName("abc", "def")
        scopes1 = ScopesType("http://myscope")
        ttype2 = QName("namespace", "myOtherTestService_type1")
        scopes2 = ScopesType("http://other_scope")

        addresses = ["http://localhost:8080/abc", 'http://{ip}/device_service']
        epr = 'my_epr'
        self.wsd_service.publish_service(epr, types=[ttype1], scopes=scopes1, x_addrs=addresses)
        time.sleep(1)

        # test that unfiltered search delivers at least my service
        test_log.info('starting search no filter...')
        services = self.wsd_client.search_services(timeout=self.SEARCH_TIMEOUT)
        myServices = [s for s in services if s.epr == epr]
        self.assertEqual(len(myServices), 1)

        # test that filtered search (types) delivers only my service
        test_log.info('starting search types filter...')
        services = self.wsd_client.search_services(types=[ttype1], timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)
        myServices = [s for s in services if s.epr == epr]
        self.assertEqual(len(myServices), 1)

        # test that filtered search (scopes) delivers only my service
        test_log.info('starting search scopes filter...')
        services = self.wsd_client.search_services(scopes=scopes1, timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)
        myServices = [s for s in services if s.epr == epr]
        self.assertEqual(len(myServices), 1)

        # test that filtered search (scopes+types) delivers only my service
        test_log.info('starting search scopes+types filter...')
        services = self.wsd_client.search_services(types=[ttype1], scopes=scopes1, timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)
        myServices = [s for s in services if s.epr == epr]
        self.assertEqual(len(myServices), 1)

        # test that filtered search (wrong type) finds no service
        test_log.info('starting search types filter...')
        services = self.wsd_client.search_services(types=[ttype2], timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 0)

        # test that filtered search (wrong scope) finds no service
        test_log.info('starting search wrong scopes filter...')
        services = self.wsd_client.search_services(scopes=scopes2, timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 0)

        # test that filtered search (correct scopes+ wrong types) finds no service
        test_log.info('starting search scopes+types filter...')
        services = self.wsd_client.search_services(types=[ttype2], scopes=scopes1, timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 0)

        # test that filtered search (wrong scopes + wrong types) finds no service
        test_log.info('starting search scopes+types filter...')
        services = self.wsd_client.search_services(types=[ttype1], scopes=scopes2, timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 0)

    def test_discover_serviceFirst(self):
        test_log.info('starting service...')
        self.wsd_service.start()

        ttype = QName("abc", "def")
        scopes = ScopesType("http://other_scope")

        addresses = ["localhost:8080/abc", ]
        epr = 'my_epr'
        self.wsd_service.publish_service(epr, types=[ttype], scopes=scopes, x_addrs=addresses)
        time.sleep(2)

        test_log.info('starting client...')
        self.wsd_client.start()
        time.sleep(0.2)
        test_log.info('starting search...')
        services = self.wsd_client.search_services(timeout=self.SEARCH_TIMEOUT)
        test_log.info('search done.')

        for service in services:
            test_log.info('found service: {} : {}'.format(service.epr, service.get_x_addrs()))
        myServices = [s for s in services if s.epr == epr]
        self.assertEqual(len(myServices), 1)

    def test_discover_noEPR(self):
        """ if a service has no epr in ProbeMatches response, it shall be ignored."""
        self.wsd_service.PROBEMATCH_EPR = False
        test_log.info('starting service...')
        self.wsd_service.start()

        ttype = QName("abc", "def")
        scopes = ScopesType("http://other_scope")

        addresses = ["localhost:8080/abc", ]
        epr = 'my_epr'
        self.wsd_service.publish_service(epr, types=[ttype], scopes=scopes, x_addrs=addresses)
        time.sleep(5)  # make sure hello messages are all sent before client discovery starts

        test_log.info('starting client...')
        self.wsd_client.start()
        time.sleep(0.2)
        test_log.info('starting search...')
        self.wsd_client.clear_remote_services()
        services = self.wsd_client.search_services(timeout=self.SEARCH_TIMEOUT)
        test_log.info('search done.')
        self.assertEqual(len(services), 0)

    def test_discover_noTYPES(self):
        """ if a service has no types in ProbeMatches response, the client shall send a resolve and add that result."""
        self.wsd_service.PROBEMATCH_TYPES = False
        test_log.info('starting wsd_service...')
        self.wsd_service.start()

        ttype = QName("abc", "def")
        scopes = ScopesType("http://other_scope")

        addresses = ["localhost:8080/abc", ]
        epr = 'my_epr'
        test_log.info('publish_service...')
        self.wsd_service.publish_service(epr, types=[ttype], scopes=scopes, x_addrs=addresses)
        time.sleep(2)

        test_log.info('starting client...')
        self.wsd_client.start()
        time.sleep(0.2)
        test_log.info('starting search...')
        services = self.wsd_client.search_services(timeout=self.SEARCH_TIMEOUT)
        test_log.info('search done.')

        for service in services:
            test_log.info('found service: {} : {}'.format(service.epr, service.get_x_addrs()))
        myServices = [s for s in services if s.epr == epr]
        self.assertEqual(len(myServices), 1)
        self.assertEqual(myServices[0].types, [ttype])

    def test_discover_noScopes(self):
        """ if a service has no scopes in ProbeMatches response, the client shall send a resolve and add that result."""
        self.wsd_service.PROBEMATCH_SCOPES = False
        test_log.info('starting service...')
        self.wsd_service.start()

        ttype = QName("abc", "def")
        scopes = ScopesType("http://other_scope")

        addresses = ["localhost:8080/abc", ]
        epr = 'my_epr'
        self.wsd_service.publish_service(epr, types=[ttype], scopes=scopes, x_addrs=addresses)
        time.sleep(2)

        test_log.info('starting client...')
        self.wsd_client.start()
        time.sleep(0.2)
        test_log.info('starting search...')
        services = self.wsd_client.search_services(timeout=self.SEARCH_TIMEOUT)
        test_log.info('search done.')

        for service in services:
            test_log.info('found service: {} : {}'.format(service.epr, service.get_x_addrs()))
        myServices = [s for s in services if s.epr == epr]
        self.assertEqual(len(myServices), 1)
        self.assertEqual(myServices[0].types, [ttype])

    def test_discover_no_x_addresses(self):
        """ if a service has no x-addresses in ProbeMatches response, the client shall send a resolve and add that result."""
        self.wsd_service.PROBEMATCH_XADDRS = False
        test_log.info('starting wsd_service...')
        self.wsd_service.start()

        ttype = QName("abc", "def")
        scopes = ScopesType("http://other_scope")

        addresses = ["localhost:8080/abc", ]
        epr = 'my_epr'
        test_log.info('publish_service...')
        self.wsd_service.publish_service(epr, types=[ttype], scopes=scopes, x_addrs=addresses)
        time.sleep(2)

        test_log.info('starting client...')
        self.wsd_client.start()
        time.sleep(0.2)
        test_log.info('starting search...')
        services = self.wsd_client.search_services(timeout=self.SEARCH_TIMEOUT)
        test_log.info('search done.')

        for service in services:
            test_log.info('found service: {} : {}'.format(service.epr, service.get_x_addrs()))
        myServices = [s for s in services if s.epr == epr]
        self.assertEqual(len(myServices), 1)
        self.assertEqual(myServices[0].types, [ttype])

    def test_ScopeMatch(self):
        my_scope = 'biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2FBed?fac=HOSP1&bldng=b1&poc=CU1&flr=1&rm=r2&bed=Bed'
        other_scopes = [(
            'biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2FBed?fac=HOSP1&bldng=b1&poc=CU1&flr=1&rm=r2&bed=Bed',
            True, 'identical'),
            (
                'biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2FBed?fac=HOSP1&bldng=b1&poc=CU1&flr=1&rm=r2&bed=NoBed',
                True, 'different query'),
            (
                'biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1/b1/CU1/1/r2/Bed?fac=HOSP1&bldng=b1&poc=CU1&flr=1&rm=r2&bed=NoBed',
                False, 'real slashes'),
            ('biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2FBed', True, 'no query'),
            ('biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2FBed/Longer', True,
             'longer target'),
            (
                'Biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2FBed?fac=HOSP1&bldng=b1&poc=CU1&flr=1&rm=r2&bed=Bed',
                True, 'different case schema'),
            (
                'biceps.ctxt.location:/Biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2FBed?fac=HOSP1&bldng=b1&poc=CU1&flr=1&rm=r2&bed=Bed',
                False, 'different case root'),
            (
                'biceps.ctxt.location:/biceps.ctxt.unknown/HOSP1%2Fb1%2FCU1%2F1%2Fr2%2Fbed?fac=HOSP1&bldng=b1&poc=CU1&flr=1&rm=r2&bed=Bed',
                False, 'different case bed'),
        ]
        matchBy = wsdiscovery.MATCH_BY_URI
        for other_scope, expected_match_result, remark in other_scopes:
            test_log.info('checking other scope {}'.format(remark))
            parsed = urlparse(other_scope)
            test_log.info('checking other scope {}'.format(parsed))
            test_log.info('urlsplit {} = {}'.format(parsed.path, urlsplit(parsed.path)))
            result = wsdiscovery.match_scope(my_scope, other_scope, matchBy)
            self.assertEqual(expected_match_result, result, msg=remark)
        # Longer matches my_scope, but not the other way round
        longer = other_scopes[4][0]
        self.assertTrue(wsdiscovery.match_scope(my_scope, longer, matchBy), msg='short scope matches longer scope')
        self.assertFalse(wsdiscovery.match_scope(longer, my_scope, matchBy),
                         msg='long scope shall not match short scope')

    def test_publishManyServices_lateStartedClient(self):
        test_log.info('starting service...')
        self.wsd_service.start()
        deviceCount = 20
        for i in range(deviceCount):
            ttype1 = QName("namespace", "myOtherTestService_type1")
            scopes1 = ScopesType("http://other_scope")

            epr = 'my_epr{}'.format(i)
            addresses = ["localhost:{}/{}".format(8080 + i, epr)]
            self.wsd_service.publish_service(epr, types=[ttype1], scopes=scopes1, x_addrs=addresses)

        time.sleep(3.02)
        test_log.info('starting client...')
        self.wsd_client.start()
        services = self.wsd_client.search_services(timeout=1)
        test_log.info('search done.')

        for service in services:
            test_log.info('found service: {} : {}'.format(service.epr, service.get_x_addrs()))
        myServices = [s for s in services if 'my_epr' in s.epr]  # there might be other devices in the network
        self.assertEqual(len(myServices), deviceCount)

    def test_publishManyServices_earlyStartedClient(self):
        # verify if client keeps track of all devices that appeared / disappeared after start without searching.
        test_log.info('starting client...')
        self.wsd_client.start()
        time.sleep(0.01)
        test_log.info('starting service...')
        self.wsd_service.start()
        deviceCount = 20
        for i in range(deviceCount):
            ttype1 = QName("namespace", "myOtherTestService_type1")
            scopes1 = ScopesType("http://other_scope")

            epr = 'my_epr{}'.format(i)
            addresses = ["localhost:{}/{}".format(8080 + i, epr)]
            self.wsd_service.publish_service(epr, types=[ttype1], scopes=scopes1, x_addrs=addresses)

        time.sleep(2.02)
        self.assertEqual(len(self.wsd_client._remote_services), deviceCount)
        test_log.info('stopping service...')
        test_log.info('clear_local_services...')
        self.wsd_service.clear_local_services()
        time.sleep(2.02)
        self.assertEqual(len(self.wsd_client._remote_services), 0)

    def test_unexpected_multicast_messages(self):
        """verify that module is robust against all kind of invalid multicast and single cast messages"""

        MULTICAST_PORT = wsdiscovery.MULTICAST_PORT
        wsdiscovery.MULTICAST_PORT = 37020  # change port, otherwise windows steals unicast messages

        address = '127.0.0.1'
        unicast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        def send_and_assert_running(data):
            unicast_sock.sendto(data.encode('utf-8'), (address, wsdiscovery.MULTICAST_PORT))
            time.sleep(0.1)
            self.assertTrue(self.wsd_service._networking_thread._recv_thread.is_alive())
            self.assertTrue(self.wsd_service._networking_thread._qread_thread.is_alive())
            self.assertTrue(self.wsd_service._networking_thread._send_thread.is_alive())
            self.assertTrue(self.wsd_service._addrs_monitor_thread.is_alive())

        self.wsd_service.start()
        time.sleep(0.1)

        try:
            self.log_watcher_service.setPaused(True)
            send_and_assert_running('no xml at all')
            send_and_assert_running(f'<bla>invalid xml fragment</bla>')
        finally:
            wsdiscovery.MULTICAST_PORT = MULTICAST_PORT
            unicast_sock.close()
            self.log_watcher_service.setPaused(False)
