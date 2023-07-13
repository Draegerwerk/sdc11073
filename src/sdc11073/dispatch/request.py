from __future__ import annotations
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from sdc11073.pysoap.msgreader import ReceivedMessage

class RequestData:
    """This class holds all information about the processing of a http request together"""

    def __init__(self,
                 http_header: dict,
                 path: str,
                 peer_name: str,
                 request: Optional[bytes] = None,
                 message_data: Optional[ReceivedMessage] = None):
        self.http_header: dict = http_header
        self.path = path
        self.peer_name: str = peer_name  # for logging
        self.request: Optional[bytes] = request
        self.message_data: Optional[ReceivedMessage] = message_data
        self.consumed_path_elements = []
        if path.startswith('/'):
            path = path[1:]
        self.path_elements = path.split('/')

    def consume_current_path_element(self):
        if len(self.path_elements) == 0:
            return None
        self.consumed_path_elements.append(self.path_elements[0])
        self.path_elements = self.path_elements[1:]
        return self.consumed_path_elements[-1]

    @property
    def current_path_element(self):
        return self.path_elements[0] if len(self.path_elements) > 0 else None
