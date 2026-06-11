"""Test for the network module."""

from __future__ import annotations

import random
import socket
import struct
import uuid
from unittest import mock

import ifaddr

from sdc11073 import network


def _create_adapter(ip: str, name: str | None = None, description: str | None = None, mask: int = 24) -> ifaddr.Adapter:
    ips = [ifaddr.IP(ip=ip, network_prefix=mask, nice_name=name or uuid.uuid4().hex)]
    return ifaddr.Adapter(name='', nice_name=description or uuid.uuid4().hex, ips=ips)


def test_correct_parsing_of_config():
    """Test whether the network adapter config is parsed correctly."""

    def random_ip() -> str:
        return socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xFFFFFFFF)))

    def get_ips(adapters_: list[ifaddr.Adapter]) -> list[str]:
        ips_ = []
        for adapter_ in adapters_:
            ips_.extend([str(ip_) for ip_ in adapter_.ips])
        return ips_

    expected_adapters = []
    while (ip := random_ip()) in get_ips(expected_adapters) and len(expected_adapters) <= 32:
        expected_adapters.append(_create_adapter(ip, mask=len(expected_adapters)))

    with mock.patch.object(ifaddr, 'get_adapters', return_value=expected_adapters):
        assert len(network.get_adapters()) == len(expected_adapters)


def test_blacklisted():
    """Test whether ips listed in the blacklist are not parsed."""
    blacklisted_adapter = [_create_adapter(ip) for ip in network.IP_BLACKLIST]
    with mock.patch.object(ifaddr, 'get_adapters', return_value=blacklisted_adapter):
        assert len(network.get_adapters()) == 0
