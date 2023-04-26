"""
Test for the netconn module.
"""

import ipaddress
import typing
import unittest
import uuid
from unittest import mock

import ifaddr

from sdc11073 import netconn


class NetworkTest(unittest.TestCase):
    """
    Test methods contained in the netconn module.
    """

    def setUp(self):
        """
        Set up platform independent test variables.
        """
        self.ip_addresses: typing.List[typing.Tuple[str, int]] = \
            [('192.168.0.1', 24), ('10.10.0.1', 32), ('127.0.0.1', 24)]
        self.expected_adapters: typing.List[ifaddr.Adapter] = []

        for ip, network_prefix in self.ip_addresses:
            ips = [ifaddr.IP(ip=ip, network_prefix=network_prefix, nice_name=f'{uuid.uuid4().hex}')]
            self.expected_adapters.append(ifaddr.Adapter(name='', nice_name=f'{uuid.uuid4().hex}', ips=ips))

    def _verify_adapter_unique_ip(self):
        """
        Verify that every adapter has a unique ip address.
        """
        self.assertEqual(len({ip.ip for ip in self._expected_ips()}), len(self.expected_adapters))

    def _expected_ips(self) -> typing.List[ifaddr.IP]:
        """
        Get the ips from the expected adapters.

        :return: ips from expected adapters.
        """
        # contains lists of ip addresses for each adapter, e.g. [['..', '...'],['...']]
        list_of_ip_lists = [adapter.ips for adapter in self.expected_adapters]

        # flattens the list of lists and creating a single list containing each entry of the sublists
        return [ip for ips in list_of_ip_lists for ip in ips]

    def test_correct_ips_from_network(self):
        """
        Test whether the correct ips are determined when providing a subnet.
        """
        self._verify_adapter_unique_ip()
        with mock.patch.object(ifaddr, 'get_adapters', return_value=self.expected_adapters):
            adapters = netconn.get_adapters()
            for adapter in adapters:
                self.assertEqual(adapter.ip, netconn.get_adapter_by_ip(adapter.ip).ip)

        # now, only one adapter with a very restricted range is available
        nice_name = uuid.uuid4().hex
        ips = [ifaddr.IP(ip='127.0.0.1', network_prefix=32, nice_name=nice_name)]
        self.expected_adapters = [ifaddr.Adapter(name='', nice_name=f'description 1', ips=ips)]

        with mock.patch.object(ifaddr, 'get_adapters', return_value=self.expected_adapters):
            netconn.get_adapter_by_ip(ipaddress.IPv4Address('127.0.0.1'))  # verify no error
            netconn.get_adapter_by_name(nice_name)  # verify no error
            with self.assertRaises(netconn.NetworkAdapterNotFoundError):
                netconn.get_adapter_by_ip(ipaddress.IPv4Address('127.0.0.2'))
            with self.assertRaises(netconn.NetworkAdapterNotFoundError):
                netconn.get_adapter_by_name(f'{nice_name}x')

    def test_correct_parsing_of_config(self):
        """
        Test whether the network adapter config is parsed correctly.
        """
        self._verify_adapter_unique_ip()
        with mock.patch.object(ifaddr, 'get_adapters', return_value=self.expected_adapters):
            adapters = netconn.get_adapters()
            self.assertEqual(len(adapters), len(self._expected_ips()))
            for expected_ip in self._expected_ips():
                expected_adapter: ifaddr.Adapter = \
                    next(adap for adap in self.expected_adapters if expected_ip.ip in [ip.ip for ip in adap.ips])
                actual_adapter: netconn.NetworkAdapter = \
                    next(adapter for adapter in adapters if str(adapter.ip) == expected_ip.ip)
                self.assertEqual(expected_ip.ip, str(actual_adapter.ip))
                self.assertEqual(expected_ip.nice_name, actual_adapter.name)
                self.assertEqual(expected_adapter.nice_name, actual_adapter.description)

    def test_error_when_multiple_ips_on_same_subnet(self):
        """
        Test whether an exception is thrown when multiple ip addresses from the same subnet are detected.
        """
        additional_ips = [('10.1.1.1', 24), ('10.1.1.2', 24)]
        ips_only = []
        for ip, network_prefix in additional_ips:
            ips = [ifaddr.IP(ip=ip, network_prefix=network_prefix, nice_name=f'{uuid.uuid4().hex}')]
            self.expected_adapters.append(ifaddr.Adapter(name='', nice_name=f'{uuid.uuid4().hex}', ips=ips))
            ips_only.append(ip)

        self._verify_adapter_unique_ip()
        with mock.patch.object(ifaddr, 'get_adapters', return_value=self.expected_adapters):
            for adapter in [ip for ip in self._expected_ips() if ip.ip not in ips_only]:
                netconn.get_adapter_by_ip(adapter.ip)  # ensure throws no error
            for adapter in [ip for ip in self._expected_ips() if ip.ip in ips_only]:
                with self.assertRaises(RuntimeError):
                    netconn.get_adapter_by_ip(adapter.ip)

    def test_multiple_ip_address_on_single_adapter(self):
        """
        Test whether multiple ip addresses on a single network interface do not throw an error.
        """
        expected_name = str(uuid.uuid4())
        expected_description = str(uuid.uuid4())

        expected_ips: typing.List[typing.Tuple[str, int]] = [('127.0.0.1', 24), ('127.0.1.1', 24)]
        ips: typing.List[ifaddr.IP] = []
        for ip, network_prefix in expected_ips:
            ips.append(ifaddr.IP(ip=ip, network_prefix=network_prefix, nice_name=expected_name))
        self.expected_adapters = [ifaddr.Adapter(name='', nice_name=expected_description, ips=ips)]

        with mock.patch.object(ifaddr, 'get_adapters', return_value=self.expected_adapters):
            for ip in [ipaddress.IPv4Address(i[0]) for i in expected_ips]:
                actual_adapter = netconn.get_adapter_by_ip(ip)
                self.assertEqual(expected_name, actual_adapter.name)
                self.assertEqual(expected_description, actual_adapter.description)
                self.assertEqual(ip, actual_adapter.ip)
