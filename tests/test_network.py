"""Test for the network module."""
from __future__ import annotations

import random
import socket
import struct
import uuid
from unittest import mock

import ifaddr
import pytest

from sdc11073 import network


def _create_adapter(ip: str, name: str | None = None, description: str | None = None, mask: int = 24) -> ifaddr.Adapter:
    ips = [ifaddr.IP(ip=ip, network_prefix=mask, nice_name=name or uuid.uuid4().hex)]
    return ifaddr.Adapter(name='', nice_name=description or uuid.uuid4().hex, ips=ips)


def test_not_found_error():
    """Test whether the correct ips are determined when providing a subnet."""
    with mock.patch.object(ifaddr, 'get_adapters', return_value=[_create_adapter('127.0.0.1', mask=32)]):
        network.get_adapter_containing_ip('127.0.0.1')  # verify no error
        with pytest.raises(network.NetworkAdapterNotFoundError):
            network.get_adapter_containing_ip('127.0.0.2')
    with mock.patch.object(ifaddr, 'get_adapters', return_value=[_create_adapter('127.0.0.1', mask=24)]):
        network.get_adapter_containing_ip('127.0.0.255')  # verify no error
        with pytest.raises(network.NetworkAdapterNotFoundError):
            network.get_adapter_containing_ip('127.0.1.1')


def test_correct_parsing_of_config():
    """Test whether the network adapter config is parsed correctly."""

    def random_ip() -> str:
        return socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))  # noqa: S311

    def get_ips(adapters_: list[ifaddr.Adapter]) -> list[str]:
        ips_ = []
        for adapter_ in adapters_:
            ips_.extend([str(ip_) for ip_ in adapter_.ips])
        return ips_

    expected_adapters = []
    while (ip := random_ip()) in get_ips(expected_adapters) and len(expected_adapters) <= 32:  # noqa: PLR2004
        expected_adapters.append(_create_adapter(ip, mask=len(expected_adapters)))

    with mock.patch.object(ifaddr, 'get_adapters', return_value=expected_adapters):
        assert len(network.get_adapters()) == len(expected_adapters)
        for expected_adapter in expected_adapters:
            for expected_ip in expected_adapter.ips:
                actual_adapter = network.get_adapter_containing_ip(expected_ip.ip)
                assert actual_adapter.ip == expected_ip.ip
                assert actual_adapter.name == expected_ip.nice_name
                assert actual_adapter.description == expected_adapter.nice_name


@pytest.mark.parametrize(
    'ips, expected',
    [
        (['10.1.1.1', '10.1.1.2', '10.1.1.3', '10.1.1.4', '10.1.1.5'], '10.1.1.3'),
        (['10.1.1.6', '10.1.1.7', '10.1.1.8', '10.1.1.9', '10.1.1.10'], '10.1.1.8'),
    ],
)
def test_closest_when_multiple_ips_on_same_subnet(ips: list[str], expected: str):
    """Test whether an exception is thrown when multiple ip addresses from the same subnet are detected."""
    with mock.patch.object(ifaddr, 'get_adapters', return_value=[_create_adapter(ip) for ip in ('10.1.1.3',
                                                                                                '10.1.1.8')]):
        for ip in ips:
            assert str(network.get_adapter_containing_ip(ip).ip) == expected


@pytest.mark.parametrize(
    'ips',
    [
        ['127.0.0.1', '127.0.0.2', '127.0.1.1'],
    ],
)
def test_multiple_ip_address_on_single_adapter(ips: list[str]):
    """Test whether multiple ip addresses on a single network interface do not throw an error."""
    expected_name = str(uuid.uuid4())
    expected_description = str(uuid.uuid4())
    ips_ = [ifaddr.IP(ip=ip, network_prefix=24, nice_name=expected_name) for ip in ips]
    expected_adapters = [ifaddr.Adapter(name='', nice_name=expected_description, ips=ips_)]

    with mock.patch.object(ifaddr, 'get_adapters', return_value=expected_adapters):
        assert len(network.get_adapters()) == len(ips)
        for ip in ips:
            actual_adapter = network.get_adapter_containing_ip(ip)
            assert actual_adapter.name == expected_name
            assert actual_adapter.description == expected_description
            assert str(actual_adapter.ip) == ip


def test_blacklisted():
    """Test whether ips listed in the blacklist are not parsed."""
    blacklisted_adapter = [_create_adapter(ip) for ip in network.IP_BLACKLIST]
    with mock.patch.object(ifaddr, 'get_adapters', return_value=blacklisted_adapter):
        assert len(network.get_adapters()) == 0
        for adapter in blacklisted_adapter:
            for ip in adapter.ips:
                with pytest.raises(network.NetworkAdapterNotFoundError):
                    network.get_adapter_containing_ip(ip.ip)
