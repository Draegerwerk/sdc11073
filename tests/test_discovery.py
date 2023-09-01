import logging
import socket
import sys
import threading
import time
import unittest
import uuid
from urllib.parse import urlparse, urlsplit

from sdc11073 import loghelper, network, wsdiscovery
from sdc11073.wsdiscovery.wsdimpl import MatchBy, match_scope
from sdc11073.xml_types.wsd_types import ScopesType
from tests import utils

test_log = logging.getLogger("unittest")

_formatter_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"


# pylint: disable=protected-access

def setUpModule():
    wsd_log = logging.getLogger("wsd_client")
    wsd_log.setLevel(logging.DEBUG)
    # create console handler and set level to debug
    sh = logging.StreamHandler()
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
    # create formatter
    formatter = logging.Formatter(_formatter_string)
    # add formatter to sh
    sh.setFormatter(formatter)
    # add ch to logger
    srv_log.addHandler(sh)

    test_log.setLevel(logging.INFO)
    # create console handler and set level to debug
    sh = logging.StreamHandler()
    # create formatter
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    # add formatter to ch
    sh.setFormatter(formatter)
    # add ch to logger
    test_log.addHandler(sh)


class TestDiscovery(unittest.TestCase):
    SEARCH_TIMEOUT = 2
    MY_MULTICAST_PORT = 37020  # change port, otherwise windows steals unicast messages

    def setUp(self):
        test_log.debug(f'setUp {self._testMethodName}')

        # give them different logger names so that output can be distinguished
        self.wsd_client = wsdiscovery.WSDiscovery('127.0.0.1',
                                                  logger=loghelper.get_logger_adapter('wsd_client'),
                                                  multicast_port=self.MY_MULTICAST_PORT)
        self.wsd_service = wsdiscovery.WSDiscovery('127.0.0.1',
                                                   logger=loghelper.get_logger_adapter('wsd_service'),
                                                   multicast_port=self.MY_MULTICAST_PORT)
        self.log_watcher_client = loghelper.LogWatcher(logging.getLogger('wsd_client'), level=logging.ERROR)
        self.log_watcher_service = loghelper.LogWatcher(logging.getLogger('wsd_service'), level=logging.ERROR)

        test_log.debug(f'setUp done{self._testMethodName}')

    def tearDown(self):
        test_log.debug(f'tearDown {self._testMethodName}')
        self.wsd_client.stop()
        self.wsd_service.stop()

        try:
            self.log_watcher_client.check()
        except loghelper.LogWatchError as ex:
            sys.stderr.write(repr(ex))
            raise
        try:
            self.log_watcher_service.check()
        except loghelper.LogWatchError as ex:
            sys.stderr.write(repr(ex))
            raise

        test_log.debug(f'tearDown done {self._testMethodName}')

    def test_invalid_address(self):
        self.assertRaises(network.NetworkAdapterNotFoundError, wsdiscovery.WSDiscovery, '128.0.0.1')

    def test_discover(self):
        test_log.info('starting client...')
        test_log.info('starting service...')

        self.wsd_service.start()
        self.wsd_client.start()
        time.sleep(0.1)

        ttype1 = [utils.random_qname()]
        scopes1 = utils.random_scope()
        ttype2 = [utils.random_qname()]
        scopes2 = utils.random_scope()

        addresses = [f"http://localhost:8080/{uuid.uuid4()}", 'http://{ip}/' + str(uuid.uuid4())]
        epr = uuid.uuid4().hex
        self.wsd_service.publish_service(epr, types=ttype1, scopes=scopes1, x_addrs=addresses)
        time.sleep(1)

        # test that unfiltered search delivers at least my service
        test_log.info('starting search no filter...')
        services = self.wsd_client.search_services(timeout=self.SEARCH_TIMEOUT)
        self.assertTrue(any(s for s in services if s.epr == epr))

        # test that filtered search (types) delivers only my service
        test_log.info('starting search types filter...')
        services = self.wsd_client.search_services(types=ttype1, timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)
        self.assertTrue(all(s for s in services if s.epr == epr))

        # test that filtered search (scopes) delivers only my service
        test_log.info('starting search scopes filter...')
        services = self.wsd_client.search_services(scopes=scopes1, timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)
        self.assertTrue(all(s for s in services if s.epr == epr))

        # test that filtered search (scopes+types) delivers only my service
        test_log.info('starting search scopes+types filter...')
        services = self.wsd_client.search_services(types=ttype1, scopes=scopes1, timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)
        self.assertTrue(all(s for s in services if s.epr == epr))

        # test that filtered search (wrong type) finds no service
        test_log.info('starting search types filter...')
        services = self.wsd_client.search_services(types=ttype2, timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 0)

        # test that filtered search (wrong scope) finds no service
        test_log.info('starting search wrong scopes filter...')
        services = self.wsd_client.search_services(scopes=scopes2, timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 0)

        # test that filtered search (correct scopes+ wrong types) finds no service
        test_log.info('starting search scopes+types filter...')
        services = self.wsd_client.search_services(types=ttype2, scopes=scopes1, timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 0)

        # test that filtered search (wrong scopes + wrong types) finds no service
        test_log.info('starting search scopes+types filter...')
        services = self.wsd_client.search_services(types=ttype1, scopes=scopes2, timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 0)

        # test search_multiple_types
        test_log.info('starting search scopes+types filter...')
        services = self.wsd_client.search_multiple_types(types_list=[ttype1, ttype2], timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 1)

        addresses2 = [f"http://localhost:8080/{uuid.uuid4()}"]
        self.wsd_service.publish_service(uuid.uuid4().hex, types=ttype2, scopes=scopes2, x_addrs=addresses2)
        time.sleep(1)

        ttype3 = [utils.random_qname()]
        self.wsd_service.publish_service(uuid.uuid4().hex,
                                         types=ttype3,
                                         scopes=utils.random_scope(),
                                         x_addrs=[f"http://localhost:8080/{uuid.uuid4()}"])
        time.sleep(1)

        # test search_multiple_types
        test_log.info('starting search scopes+types filter...')
        services = self.wsd_client.search_multiple_types(types_list=[ttype1, ttype2], timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 2)

        # test search_multiple_types
        test_log.info('starting search scopes+types filter...')
        services = self.wsd_client.search_multiple_types(types_list=[ttype1, ttype2, ttype3],
                                                         timeout=self.SEARCH_TIMEOUT)
        self.assertEqual(len(services), 3)

    def test_discover_serviceFirst(self):
        test_log.info('starting service...')
        self.wsd_service.start()

        epr = uuid.uuid4().hex
        self.wsd_service.publish_service(epr,
                                         types=[utils.random_qname()],
                                         scopes=utils.random_scope(),
                                         x_addrs=[uuid.uuid4().hex])
        time.sleep(2)

        test_log.info('starting client...')
        self.wsd_client.start()
        time.sleep(0.2)
        test_log.info('starting search...')
        services = self.wsd_client.search_services(timeout=self.SEARCH_TIMEOUT)
        test_log.info('search done.')

        for service in services:
            test_log.info(f'found service: {service.epr} : {service.x_addrs}')
        self.assertEqual(len([s for s in services if s.epr == epr]), 1)

    def test_discover_noEPR(self):
        """If a service has no epr in ProbeMatches response, it shall be ignored."""
        self.wsd_service.PROBEMATCH_EPR = False
        test_log.info('starting service...')
        self.wsd_service.start()

        ttypes = [utils.random_qname()]
        scopes = ScopesType(utils.random_location().scope_string)

        addresses = [f"localhost:8080/{uuid.uuid4()}"]
        epr = uuid.uuid4().hex
        self.wsd_service.publish_service(epr, types=ttypes, scopes=scopes, x_addrs=addresses)
        time.sleep(5)  # make sure hello messages are all sent before client discovery starts

        test_log.info('starting client...')
        self.wsd_client.start()
        time.sleep(0.2)
        test_log.info('starting search...')
        self.wsd_client.clear_remote_services()
        services = self.wsd_client.search_services(timeout=self.SEARCH_TIMEOUT, types=ttypes, scopes=scopes)
        test_log.info('search done.')
        self.assertEqual(len(services), 0)

    def test_discover_noTYPES(self):
        """If a service has no types in ProbeMatches response, the client shall send a resolve and add that result."""
        self.wsd_service.PROBEMATCH_TYPES = False
        test_log.info('starting wsd_service...')
        self.wsd_service.start()

        ttype = utils.random_qname()

        addresses = ["localhost:8080/abc"]
        epr = uuid.uuid4().hex
        test_log.info('publish_service...')
        self.wsd_service.publish_service(epr, types=[ttype], scopes=utils.random_scope(), x_addrs=addresses)
        time.sleep(2)

        test_log.info('starting client...')
        self.wsd_client.start()
        time.sleep(0.2)
        test_log.info('starting search...')
        services = self.wsd_client.search_services(timeout=self.SEARCH_TIMEOUT)
        test_log.info('search done.')

        for service in services:
            test_log.info(f'found service: {service.epr} : {service.x_addrs}')
        my_services = [s for s in services if s.epr == epr]
        self.assertEqual(len(my_services), 1)
        self.assertEqual(my_services[0].types, [ttype])

    def test_discover_noScopes(self):
        """If a service has no scopes in ProbeMatches response, the client shall send a resolve and add that result."""
        self.wsd_service.PROBEMATCH_SCOPES = False
        test_log.info('starting service...')
        self.wsd_service.start()

        ttype = utils.random_qname()

        epr = uuid.uuid4().hex
        addresses = [f"localhost:8080/{uuid.uuid4()}"]
        self.wsd_service.publish_service(epr, types=[ttype], scopes=utils.random_scope(), x_addrs=addresses)
        time.sleep(2)

        test_log.info('starting client...')
        self.wsd_client.start()
        time.sleep(0.2)
        test_log.info('starting search...')
        services = self.wsd_client.search_services(timeout=self.SEARCH_TIMEOUT)
        test_log.info('search done.')

        for service in services:
            test_log.info(f'found service: {service.epr} : {service.x_addrs}')
        my_services = [s for s in services if s.epr == epr]
        self.assertEqual(len(my_services), 1)
        self.assertEqual(my_services[0].types, [ttype])

    def test_discover_no_x_addresses(self):
        """If a service has no x-addresses in ProbeMatches response, the client shall send a resolve and add that result."""
        self.wsd_service.PROBEMATCH_XADDRS = False
        test_log.info('starting wsd_service...')
        self.wsd_service.start()

        ttype = utils.random_qname()
        scopes = utils.random_scope()

        epr = uuid.uuid4().hex
        test_log.info('publish_service...')
        self.wsd_service.publish_service(epr, types=[ttype], scopes=scopes, x_addrs=[f"localhost:8080/{uuid.uuid4()}"])
        time.sleep(2)

        test_log.info('starting client...')
        self.wsd_client.start()
        time.sleep(0.2)
        test_log.info('starting search...')
        services = self.wsd_client.search_services(timeout=self.SEARCH_TIMEOUT)
        test_log.info('search done.')

        for service in services:
            test_log.info(f'found service: {service.epr} : {service.x_addrs}')
        my_services = [s for s in services if s.epr == epr]
        self.assertEqual(len(my_services), 1)
        self.assertEqual(my_services[0].types, [ttype])

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
        match_by = MatchBy.uri
        for other_scope, expected_match_result, remark in other_scopes:
            test_log.info(f'checking other scope {remark}')
            parsed = urlparse(other_scope)
            test_log.info(f'checking other scope {parsed}')
            test_log.info(f'urlsplit {parsed.path} = {urlsplit(parsed.path)}')
            result = match_scope(my_scope, other_scope, match_by)
            self.assertEqual(expected_match_result, result, msg=remark)
        # Longer matches my_scope, but not the other way round
        longer = other_scopes[4][0]
        self.assertTrue(match_scope(my_scope, longer, match_by), msg='short scope matches longer scope')
        self.assertFalse(match_scope(longer, my_scope, match_by),
                         msg='long scope shall not match short scope')

    def test_publishManyServices_lateStartedClient(self):
        test_log.info('starting service...')
        self.wsd_service.start()
        device_count = 20
        eprs = [uuid.uuid4().hex for _ in range(device_count)]
        for i, epr in enumerate(eprs):
            self.wsd_service.publish_service(epr,
                                             types=[utils.random_qname()],
                                             scopes=utils.random_scope(),
                                             x_addrs=[f"localhost:{8080 + i}/{uuid.uuid4()}"])

        time.sleep(3.02)
        test_log.info('starting client...')
        self.wsd_client.start()
        services = self.wsd_client.search_services(timeout=self.SEARCH_TIMEOUT)
        test_log.info('search done.')

        for service in services:
            test_log.info(f'found service: {service.epr} : {service.x_addrs}')
        my_services = [s for s in services if s.epr in eprs]  # there might be other devices in the network
        self.assertEqual(len(my_services), device_count)

    def test_publishManyServices_earlyStartedClient(self):
        # verify if client keeps track of all devices that appeared / disappeared after start without searching.
        test_log.info('starting client...')
        self.wsd_client.start()
        time.sleep(0.01)
        test_log.info('starting service...')
        self.wsd_service.start()
        device_count = 20
        eprs = [uuid.uuid4().hex for _ in range(device_count)]
        epr_event_map: dict[str, threading.Event] = {epr: threading.Event() for epr in eprs}
        for i, epr in enumerate(eprs):
            addresses = [f"localhost:{8080 + i}/{epr}"]
            self.wsd_service.publish_service(epr,
                                             types=[utils.random_qname()],
                                             scopes=ScopesType(utils.random_location().scope_string),
                                             x_addrs=addresses)

        time.sleep(2.02)
        self.assertEqual(len([epr for epr in self.wsd_client._remote_services if epr in eprs]), device_count)
        test_log.info('stopping service...')
        test_log.info('clear_local_services...')

        def _remote_service_bye(_: str, epr_: str) -> None:
            if epr_ in eprs:
                self.assertTrue(epr_ not in self.wsd_client._remote_services)
                epr_event_map[epr_].set()

        self.wsd_client.set_remote_service_bye_callback(_remote_service_bye)
        self.wsd_service.clear_local_services()
        for event in epr_event_map.values():
            self.assertTrue(event.wait(timeout=5.0))

    def test_unexpected_multicast_messages(self):
        """Verify that module is robust against all kind of invalid multicast and single cast messages."""
        address = '127.0.0.1'
        unicast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

        def send_and_assert_running(data):
            unicast_sock.sendto(data.encode('utf-8'), (address, self.MY_MULTICAST_PORT))
            time.sleep(0.1)
            self.assertTrue(self.wsd_service._networking_thread._recv_thread.is_alive())
            self.assertTrue(self.wsd_service._networking_thread._qread_thread.is_alive())
            self.assertTrue(self.wsd_service._networking_thread._send_thread.is_alive())

        self.wsd_service.start()
        time.sleep(0.1)

        try:
            self.log_watcher_service.setPaused(True)
            send_and_assert_running('no xml at all')
            send_and_assert_running('<bla>invalid xml fragment</bla>')
        finally:
            unicast_sock.close()
            self.log_watcher_service.setPaused(False)
