from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable
from threading import Lock
from sdc11073 import loghelper

if TYPE_CHECKING:
    from .soapclient import SoapClientProtocol

    _SoapClientFactory = Callable[[str, list[str]], SoapClientProtocol]


class _SoapClientEntry:
    def __init__(self, soap_client: SoapClientProtocol | None, usr_ident: Any):
        self.soap_client = soap_client
        self.usr_idents = [usr_ident]


class SoapClientPool:
    """Pool of soap clients with reference count."""

    def __init__(self, soap_client_factory: _SoapClientFactory, log_prefix: str):
        self._soap_client_factory = soap_client_factory
        self._soap_clients: dict[str, _SoapClientEntry] = {}
        self._logger = loghelper.get_logger_adapter('sdc.device.soap_client_pool', log_prefix)
        self.async_loop_subscr_mgr = None  # is set by async subscription manager
        self._lock = Lock()

    def get_soap_client(self, netloc: str,
                        accepted_encodings: list[str],
                        usr_ident: Any) -> SoapClientProtocol:
        """Return a soap client for netloc.

        Method creates a new soap client if it did not exist yet.
        It also associates the user_ref (subscription) to the network location.
        """
        self._logger.debug('requested soap client for netloc {}', netloc)  # noqa: PLE1205
        with self._lock:
            entry = self._soap_clients.get(netloc)
            if entry is None:
                soap_client = self._soap_client_factory(netloc, accepted_encodings)
                entry = _SoapClientEntry(soap_client, usr_ident)
                self._soap_clients[netloc] = entry
            elif usr_ident not in entry.usr_idents:
                entry.usr_idents.append(usr_ident)
            return entry.soap_client

    def forget_usr(self, netloc: str, usr_ident: Any) -> None:
        """Remove the user reference from the network location.

        If no more associations exist, the soap connection gets closed and the soap client deleted.
        """
        self._logger.info('forget_usr called for netloc {} usr_ident {}', netloc, usr_ident)  # noqa: PLE1205
        with self._lock:
            entry = self._soap_clients.get(netloc)
            if entry is None or len(entry.usr_idents) == 0:
                # nothing to do
                return
            if usr_ident in entry.usr_idents:
                entry.usr_idents.remove(usr_ident)
                self._logger.info('forget user ref for netloc {}, {} user refs remaining',  # noqa: PLE1205
                                  netloc, len(entry.usr_idents))
            if len(entry.usr_idents) == 0:
                if entry.soap_client is not None:
                    self._logger.info('close soap client for netloc {}', netloc)  # noqa: PLE1205
                    if self.async_loop_subscr_mgr is None:
                        entry.soap_client.close()
                    else:
                        self.async_loop_subscr_mgr.run_coro(entry.soap_client.async_close())
                self._soap_clients.pop(netloc)

    def close_all(self):
        """Close all connections."""
        with self._lock:
            for entry in self._soap_clients.values():
                entry.soap_client.close()
            self._soap_clients = {}
        if self.async_loop_subscr_mgr is not None:
            self.async_loop_subscr_mgr.stop()
