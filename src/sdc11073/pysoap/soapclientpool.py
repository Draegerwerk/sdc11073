from __future__ import annotations

from typing import TYPE_CHECKING, Callable

from sdc11073 import loghelper

if TYPE_CHECKING:
    from .soapclient import SoapClientProtocol

    _SoapClientFactory = Callable[[str, list[str]], SoapClientProtocol]

_UnreachableCallback = Callable[[], None]


class _SoapClientEntry:
    def __init__(self, soap_client: SoapClientProtocol | None, unreachable_callback: _UnreachableCallback):
        self.soap_client = soap_client
        self.callbacks = [unreachable_callback]


class SoapClientPool:
    """Pool of soap clients with reference count."""

    # ToDo: distinguish between unreachable netloc and unreachable epr

    def __init__(self, soap_client_factory: _SoapClientFactory, log_prefix: str):
        self._soap_client_factory = soap_client_factory
        self._soap_clients: dict[str, _SoapClientEntry] = {}
        self._logger = loghelper.get_logger_adapter('sdc.device.soap_client_pool', log_prefix)

    def register_netloc_user(self, netloc: str, unreachable_callback: _UnreachableCallback) -> None:
        """Associate a user_ref (subscription) to a network location."""
        self._logger.debug('registered netloc {} ', netloc)  # noqa: PLE1205
        entry = self._soap_clients.get(netloc)
        if entry is None:
            # for now only register the callback, the soap client will be created later on get_soap_client call.
            self._soap_clients[netloc] = _SoapClientEntry(None, unreachable_callback)
            return
        if unreachable_callback not in entry.callbacks:
            entry.callbacks.append(unreachable_callback)

    def get_soap_client(self, netloc: str,
                        accepted_encodings: list[str],
                        unreachable_callback: _UnreachableCallback) -> SoapClientProtocol:
        """Return a soap client for netloc.

        Method creates a new soap client if it did not exist yet.
        It also associates the user_ref (subscription) to the network location.
        """
        self._logger.debug('requested soap client for netloc {}', netloc)  # noqa: PLE1205
        entry = self._soap_clients[netloc]
        if entry.soap_client is None:
            soap_client = self._soap_client_factory(netloc, accepted_encodings)
            entry.soap_client = soap_client
            return soap_client
        if unreachable_callback not in entry.callbacks:
            entry.callbacks.append(unreachable_callback)
        return entry.soap_client

    def forget_callable(self, netloc: str, unreachable_callback: _UnreachableCallback) -> None:
        """Remove the user reference from the network location.

        If no more associations exist, the soap connection gets closed and the soap client deleted.
        """
        self._logger.debug('forget soap client for netloc {}', netloc)  # noqa: PLE1205
        entry = self._soap_clients.get(netloc)
        if entry is None:
            return
        entry.callbacks.remove(unreachable_callback)
        if len(entry.callbacks) == 0:
            if entry.soap_client is not None:
                entry.soap_client.close()
            self._soap_clients.pop(netloc)

    def report_unreachable(self, netloc: str) -> None:
        """All user references for the unreachable network location will be informed.

        Then soap client gets closed and deleted.
        """
        self._logger.debug('unreachable netloc {}', netloc)  # noqa: PLE1205
        try:
            entry = self._soap_clients.pop(netloc)
        except KeyError:
            return
        entry.soap_client.close()
        for callback in entry.callbacks:
            callback()

    def close_all(self):
        """Close all connections."""
        for entry in self._soap_clients.values():
            entry.soap_client.close()
        self._soap_clients = {}
