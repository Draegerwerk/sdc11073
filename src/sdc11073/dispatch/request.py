"""Data container for an incoming http request."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sdc11073.pysoap.msgreader import ReceivedMessage


class RequestData:
    """Hold all information about the processing of a http request together."""

    def __init__(
        self,
        http_header: dict,
        path: str,
        peer_name: str,
        request: bytes | None = None,
        message_data: ReceivedMessage | None = None,
    ):
        self.http_header: dict = http_header
        self.path = path
        self.peer_name: str = peer_name  # for logging
        self.request: bytes | None = request
        self.message_data: ReceivedMessage | None = message_data
        self.consumed_path_elements = []
        path = path.removeprefix('/')
        self.path_elements = path.split('/')

    def consume_current_path_element(self) -> str | None:
        """Return the current path element and move it to the consumed elements.

        :return: the consumed path element, or None if no element is left.
        """
        if len(self.path_elements) == 0:
            return None
        self.consumed_path_elements.append(self.path_elements[0])
        self.path_elements = self.path_elements[1:]
        return self.consumed_path_elements[-1]

    @property
    def current_path_element(self) -> str | None:
        """Return the current (not yet consumed) path element, or None if none is left."""
        return self.path_elements[0] if len(self.path_elements) > 0 else None
