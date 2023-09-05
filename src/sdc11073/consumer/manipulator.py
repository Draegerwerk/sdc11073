from __future__ import annotations

from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from sdc11073 import xml_utils

    from sdc11073.pysoap.soapenvelope import Soap12Envelope


class RequestManipulatorProtocol(Protocol):
    """A request manipulator can be used to inspect or manipulate output created by the sdc client.

    Output creation is done in three steps: first a Soap12Envelope is created,
    then a (libxml) etree is created from its content, anf finally a bytestring is generated from the etree.
    The sdc client calls corresponding methods of the manipulator object after every step.
    If the method returns something different from None, this returned value will be used as input for the next step.
    """

    def manipulate_soapenvelope(self, soap_envelope: Soap12Envelope) -> Soap12Envelope | None:
        """Manipulate on Soap12Envelope level."""

    def manipulate_domtree(self, domtree: xml_utils.LxmlElement) -> xml_utils.LxmlElement | None:
        """Manipulate on etree.Element level."""

    def manipulate_string(self, xml_string: str) -> str | None:
        """Manipulate on string level."""
