from __future__ import annotations

import logging
import threading
import traceback

from ..netconn import get_ipv4_addresses

_NETWORK_ADDRESSES_CHECK_TIMEOUT = 5


class AddressMonitorThread(threading.Thread):
    """ This thread frequently checks the available Network adapters.
    Any change is reported vis wsd._network_address_removed or wsd._network_address_added
    """

    def __init__(self, wsd):
        self._addresses = set()
        self._wsd = wsd
        self._logger = logging.getLogger('sdc.discover.monitor')
        self._quit_event = threading.Event()
        super().__init__(name='AddressMonitorThread')
        self.daemon = True
        self._update_addresses()

    def _update_addresses(self):
        addresses = set(get_ipv4_addresses())

        disappeared = self._addresses.difference(addresses)
        new = addresses.difference(self._addresses)

        for address in disappeared:
            self._wsd._network_address_removed(address)

        for address in new:
            self._wsd._network_address_added(address)
        self._addresses = addresses

    def run(self):
        try:
            while not self._quit_event.wait(_NETWORK_ADDRESSES_CHECK_TIMEOUT):
                self._update_addresses()
        except Exception:
            self._logger.error('Unhandled Exception at thread runtime. Thread will abort! %s',
                               traceback.format_exc())
            raise

    def schedule_stop(self):
        """Schedule stopping the thread.
        Use join() to wait, until thread really has been stopped
        """
        self._quit_event.set()
