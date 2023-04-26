"""This module contains functions to get the hosts network adapters and ip addresses."""

import ipaddress
import typing

import ifaddr

_IP_BLACKLIST = ("0.0.0.0", None)  # None can happen if an adapter does not have any IP address


class NetworkAdapterNotFoundError(Exception):
    """Exception raised if no network adapter is found."""

    def __init__(self, available_adapters: typing.List["NetworkAdapter"], *args):
        """
        :param available_adapters: adapters available on the host
        """
        super().__init__(*args)
        self.available_adapters = available_adapters


class NetworkAdapter(ipaddress.IPv4Interface):
    """Represents a network adapter."""

    def __init__(self, name: str, description: str, address: str, network_prefix: str):
        """
        :param name: name of the interface
        :param description: descriptive, more general name of the network adapter
        :param address: ip address of the network adapter
        :param network_prefix: network prefix.
        """
        super().__init__(f"{address}/{network_prefix}")
        self.name: str = name
        self.description: str = description

    def __str__(self) -> str:
        return f"{self.name}: {super().__str__()}"


def get_adapters() -> typing.List[NetworkAdapter]:
    """
    Get all active and connected host network adapters.

    :return: list of enabled and connected network adapters on the host
    """
    adapters: typing.List[NetworkAdapter] = []
    for adapter in ifaddr.get_adapters():
        for ip in adapter.ips:
            if ip.is_IPv4:
                adapters.append(NetworkAdapter(name=ip.nice_name,
                                               description=adapter.nice_name,
                                               address=ip.ip,
                                               network_prefix=str(ip.network_prefix)))

    return adapters


def get_adapter_by_ip(ip: typing.Union[ipaddress.IPv4Address, str]) -> NetworkAdapter:
    """
    Get host network adapter containing the specified ip address in its network range.

    :param ip: ip address from which the adapter is to be determined
    :return: host network adapter containing the specified ip address in its network range
    :raise NetworkAdapterNotFoundError if no network adapter contains the specified ip
    :raise RuntimeError if multiple network adapter containing the specified ip
    """
    ip = ipaddress.IPv4Address(ip) if isinstance(ip, str) else ip

    adapters = get_adapters()
    filtered_adapters = [adapter for adapter in adapters if ip in adapter.network]

    if not filtered_adapters:
        raise NetworkAdapterNotFoundError(adapters, f'No host network adapter found that contains ip address "{ip}"')

    if len(filtered_adapters) > 1:
        raise RuntimeError(f'Found multiple host network adapters for ip address "{ip}". '
                           f'Please disable one of the following adapters: {filtered_adapters}')

    return filtered_adapters[0]


def get_adapter_by_name(name: str) -> NetworkAdapter:
    """
    Get host network adapter by the specified name.

    :param name: name of the adapter is to be determined
    :return: host network adapter with the specified name
    :raise NetworkAdapterNotFoundError if no network adapter has the specified name
    :raise RuntimeError if multiple network adapter have the specified name
    """
    adapters = get_adapters()
    filtered_adapters = [adapter for adapter in adapters if adapter.name == name]

    if not filtered_adapters:
        raise NetworkAdapterNotFoundError(adapters, f'No host network adapter found with the name "{name}"')

    if len(filtered_adapters) > 1:
        raise RuntimeError(f'Found multiple host network adapters with the name "{name}". '
                           f'Please disable one of the following adapters: {filtered_adapters}')

    return filtered_adapters[0]
