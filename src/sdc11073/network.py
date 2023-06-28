"""Get the hosts network adapters and ip addresses."""
from __future__ import annotations

import ipaddress

import ifaddr

IP_BLACKLIST = ('0.0.0.0',  # noqa: S104
                None)  # None can happen if an adapter does not have any IP address associated


class NetworkAdapterNotFoundError(Exception):
    """Exception when no network adapter is found."""

    def __init__(self, ip: ipaddress.IPv4Address, *args, **kwargs):  # noqa: ANN002 ANN003
        super().__init__(args, kwargs)
        self.ip = ip


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
        for ip in adapter.ips:
            if ip.is_IPv4 and ip.ip not in IP_BLACKLIST:
                adapters.append(NetworkAdapter(name=ip.nice_name,
                                               description=adapter.nice_name,
                                               address=ip.ip,
                                               network_prefix=ip.network_prefix))

    return adapters


def get_adapter_containing_ip(ip: ipaddress.IPv4Address | str) -> NetworkAdapter:
    """Get host network adapter containing the specified ip address in its network range.

    :param ip: ip address from which the adapter is to be determined
    :return: host network adapter containing the specified ip address in its network range
    :raise NetworkAdapterNotFoundError: no network adapter contains the specified ip
    :raise RuntimeError: multiple network adapter containing the specified ip
    """
    ip = ipaddress.IPv4Address(ip) if isinstance(ip, str) else ip

    adapters = get_adapters()
    filtered_adapters = [adapter for adapter in adapters if ip in adapter.network]

    if not filtered_adapters:
        raise NetworkAdapterNotFoundError(ip, f'No network adapter contains ip address "{ip}". Detected {adapters}')

    if len(filtered_adapters) > 1:
        # any ip address could be taken but do not choose randomly
        # sort ip addresses to determine the closest one
        filtered_adapters.sort(key=lambda a: abs(int(a) - int(ip)))

    return filtered_adapters[0]
