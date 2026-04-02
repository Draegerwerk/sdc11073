"""Get the hosts network adapters and ip addresses."""

from __future__ import annotations

import ipaddress

import ifaddr

IP_BLACKLIST = (
    '0.0.0.0',  # noqa: S104
    None,
)  # None can happen if an adapter does not have any IP address associated


class NetworkAdapter(ipaddress.IPv4Interface):
    """Represents a network adapter."""

    def __init__(self, name: str, description: str, address: str, network_prefix: str | int):
        """Create a network adapter instance.

        :param name: name of the interface
        :param description: descriptive, more general name of the network adapter
        :param address: ip address of the network adapter
        :param network_prefix: network prefix
        """
        super().__init__(f'{address}/{network_prefix}')
        self.name: str = name
        self.description: str = description

    def __str__(self) -> str:
        return f'{self.name}: {super().__str__()}'

    def __repr__(self) -> str:
        return f'{self.name}: {super().__str__()} ({self.description})'


def get_adapters() -> list[NetworkAdapter]:
    """Get all active and connected host network adapters.

    :return: list of enabled and connected network adapters on the host
    """
    adapters: list[NetworkAdapter] = []
    for adapter in ifaddr.get_adapters():
        adapters.extend(
            [
                NetworkAdapter(
                    name=ip.nice_name, description=adapter.nice_name, address=ip.ip, network_prefix=ip.network_prefix
                )
                for ip in adapter.ips
                if ip.is_IPv4 and ip.ip not in IP_BLACKLIST
            ]
        )
    return adapters
