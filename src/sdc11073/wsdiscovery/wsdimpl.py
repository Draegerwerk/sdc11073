from __future__ import annotations

import re

from .wsdbase import WSDiscoveryBase
from ..netconn import get_ip_for_adapter, get_ipv4_ips


class WSDiscoveryBlacklist(WSDiscoveryBase):
    """ Binds to all IP addresses except the black listed ones. """

    def __init__(self, ignored_adaptor_addresses=None, logger=None, multicast_port=None):
        """
        :param ignored_adaptor_addresses: an optional list of (own) ip addresses that shall not be used for discovery.
                                          IP addresses are handled as regular expressions.
        """
        super().__init__(logger, multicast_port)
        tmp = [] if ignored_adaptor_addresses is None else ignored_adaptor_addresses
        self._ignored_adaptor_addresses = [re.compile(x) for x in tmp]

    def _is_accepted_address(self, address):
        """ check if any of the regular expressions matches the argument"""
        for ign_address in self._ignored_adaptor_addresses:
            if ign_address.match(address) is not None:
                return False
        return True


class WSDiscoveryWhitelist(WSDiscoveryBase):
    """ Binds to all IP listed IP addresses. """

    def __init__(self, accepted_adapter_addresses, logger=None, multicast_port=None):
        """
        :param accepted_adapter_addresses: an optional list of (own) ip addresses that shall not be used for discovery.
        """
        super().__init__(logger, multicast_port)
        tmp = [] if accepted_adapter_addresses is None else accepted_adapter_addresses
        self.accepted_adapter_addresses = [re.compile(x) for x in tmp]

    def _is_accepted_address(self, address):
        """ check if any of the regular expressions matches the argument"""
        for acc_address in self.accepted_adapter_addresses:
            if acc_address.match(address) is not None:
                return True
        return False


class WSDiscoverySingleAdapter(WSDiscoveryBase):
    """ Bind to a single adapter, identified by name.
    """

    def __init__(self, adapter_name, logger=None, force_adapter_name=False, multicast_port=None):
        """
        :param adapter_name: a string,  e.g. 'local area connection'.
                            parameter is only relevant if host has more than one adapter or forceName is True
                            If host has more than one adapter, the adapter with this friendly name is used, but if it does not exist, a RuntimeError is thrown.
        :param logger: use this logger. If none, 'sdc.discover' is used.
        :param force_adapter_name: if True, only this named adapter will be used.
                                 If False, and only one Adapter exists, the one existing adapter is used. (localhost is ignored in this case).
        """
        super().__init__(logger, multicast_port)
        self._my_ip_address = get_ip_for_adapter(adapter_name)

        if self._my_ip_address is None:
            all_adapters = get_ipv4_ips()
            all_adapter_names = [ip.nice_name for ip in all_adapters]
            if force_adapter_name:
                raise RuntimeError(f'No adapter "{adapter_name}" found. Having {all_adapter_names}')

            # see if there is only one physical adapter. if yes, use it
            adapters_not_localhost = [a for a in all_adapters if not a.ip.startswith('127.')]
            if len(adapters_not_localhost) == 1:
                self._my_ip_address = (adapters_not_localhost[0].ip,)  # a tuple
            else:
                raise RuntimeError(f'No adapter "{adapter_name}" found. Having {all_adapter_names}')

    def _is_accepted_address(self, address):
        """ check if any of the regular expressions matches the argument"""
        return address in self._my_ip_address
