"""The RequestManipulator allows manipulation of SOAP messages, XML DOM trees, and XML strings using callback functions.

Classes:
    - RequestManipulator: An example implementation of the `RequestManipulatorProtocol`.

Functions:
    - manipulate_soapenvelope: Manipulates a SOAP message.
    - manipulate_domtree: Manipulates an XML DOM tree.
    - manipulate_string: Manipulates an XML string.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Callable

    from sdc11073 import xml_utils
    from sdc11073.pysoap.soapenvelope import Soap12Envelope


class RequestManipulator:
    """Example implementation of RequestManipulatorProtocol."""

    def __init__(
        self,
        cb_soapenvelope: Callable[[Soap12Envelope], Soap12Envelope] | None = None,
        cb_xml: Callable[[xml_utils.LxmlElement], xml_utils.LxmlElement] | None = None,
        cb_string: Callable[[str], str] | None = None,
    ):
        self.cb_soapenvelope = cb_soapenvelope
        self.cb_xml = cb_xml
        self.cb_string = cb_string

    def manipulate_soapenvelope(self, soap_envelope: Soap12Envelope) -> Soap12Envelope | None:
        """Manipulate the soap envelope."""
        if callable(self.cb_soapenvelope):
            return self.cb_soapenvelope(soap_envelope)
        return None

    def manipulate_domtree(self, domtree: xml_utils.LxmlElement) -> xml_utils.LxmlElement | None:
        """Manipulate the XML domtree."""
        if callable(self.cb_xml):
            return self.cb_xml(domtree)
        return None

    def manipulate_string(self, xml_string: str) -> str | None:
        """Manipulate the XML string."""
        if callable(self.cb_string):
            return self.cb_string(xml_string)
        return None
