from typing import Protocol, Callable, Any


_SoapClientFactory = Callable[[str, list[str]], Any]


class UserRef(Protocol):
    def on_unreachable(self, netloc):
        ...



class _SoapClientEntry:
    def __init__(self, soap_client, user_ref: UserRef):
        self.soap_client = soap_client
        self.user_refs = [user_ref]


class SoapClientPool:
    """pool of soap clients with reference count"""

    def __init__(self, soap_client_factory: _SoapClientFactory):
        self._soap_client_factory = soap_client_factory
        self._soap_clients = {}

    def get_soap_client(self, netloc: str, accepted_encodings: list[str], user_ref: Any) -> Any:
        entry = self._soap_clients.get(netloc)
        if entry is None:
            soap_client = self._soap_client_factory(netloc, accepted_encodings)
            self._soap_clients[netloc] = _SoapClientEntry(soap_client, user_ref)
            return soap_client
        if user_ref not in entry.user_refs:
            entry.user_refs.append(user_ref)
        return entry.soap_client

    def forget(self, netloc: str, user_ref: Any) -> None:
        entry = self._soap_clients.get(netloc)
        if entry is None:
            raise ValueError
        entry.user_refs.remove(user_ref)
        if len(entry.user_refs) == 0:
            entry.soap_client.close()
            self._soap_clients.pop(netloc)

    def report_unreachable(self, netloc: str) -> None:
        entry = self._soap_clients.get(netloc)
        if entry is None:
            return
        for user_ref in entry.user_refs:
            user_ref.on_unreachable(netloc)
        self._soap_clients.pop(netloc)

    def close_all(self):
        for entry in self._soap_clients.values():
            entry.soap_client.close()
        self._soap_clients = {}
