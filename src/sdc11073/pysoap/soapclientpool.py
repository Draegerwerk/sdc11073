from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable, Protocol

from sdc11073 import loghelper

if TYPE_CHECKING:
    from .soapclient import SoapClientProtocol

_SoapClientFactory = Callable[[str, list[str]], Any]


class UserRef(Protocol):
    """Reference counter that notifies reference holders when net location is unreachable."""

    def on_unreachable(self):
        """Notify net location is unreachable."""


class _SoapClientEntry:
    def __init__(self, soap_client: SoapClientProtocol | None, user_ref: UserRef):
        self.soap_client = soap_client
        self.user_refs = [user_ref]


class SoapClientPool:
    """Pool of soap clients with reference count."""

    def __init__(self, soap_client_factory: _SoapClientFactory, log_prefix: str):
        self._soap_client_factory = soap_client_factory
        self._soap_clients = {}
        self._logger = loghelper.get_logger_adapter('sdc.device.soap_client_pool', log_prefix)

    def register_netloc_user(self, netloc: str, user_ref: UserRef) -> None:
        """Associate a user_ref (subscription) to a network location."""
        self._logger.debug('registered netloc {} user {}', netloc, user_ref)  # noqa: PLE1205
        entry = self._soap_clients.get(netloc)
        if entry is None:
            self._soap_clients[netloc] = _SoapClientEntry(None, user_ref)
            return
        if user_ref not in entry.user_refs:
            entry.user_refs.append(user_ref)

    def get_soap_client(self, netloc: str, accepted_encodings: list[str], user_ref: UserRef) -> Any:
        """Return a soap client for netloc.

        Method creates a new soap client if it did not exist yet.
        It also associates the user_ref (subscription) to the network location.
        """
        self._logger.debug('requested soap client for netloc {} user {}', netloc, user_ref)  # noqa: PLE1205
        entry = self._soap_clients[netloc]
        if entry.soap_client is None:
            soap_client = self._soap_client_factory(netloc, accepted_encodings)
            entry.soap_client = soap_client
            return soap_client
        if user_ref not in entry.user_refs:
            entry.user_refs.append(user_ref)
        return entry.soap_client

    def forget(self, netloc: str, user_ref: Any) -> None:
        """Remove the user reference from the network location.

        If no more associations exist, the soap connection gets closed and the soap client deleted.
        """
        self._logger.debug('forget soap client for netloc {} user {}', netloc, user_ref)  # noqa: PLE1205
        entry = self._soap_clients.get(netloc)
        if entry is None:
            return
        entry.user_refs.remove(user_ref)
        if len(entry.user_refs) == 0:
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
        for user_ref in entry.user_refs:
            self._logger.debug('call on_unreachable netloc {} user {}', netloc, user_ref)  # noqa: PLE1205
            user_ref.on_unreachable()

    def close_all(self):
        """Close all connections."""
        for entry in self._soap_clients.values():
            entry.soap_client.close()
        self._soap_clients = {}
