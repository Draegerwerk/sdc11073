from __future__ import annotations
from typing import Protocol, TYPE_CHECKING, Union

if TYPE_CHECKING:
    from lxml.etree import Element
    from sdc11073.pysoap.soapenvelope import Soap12Envelope


class RequestManipulatorProtocol(Protocol):
    """
    A request manipulator can be used to inspect or manipulate output created by the sdc client.
    Output creation is done in three steps: first a Soap12Envelope is created,
    then a (libxml) etree is created from its content, anf finally a bytestring is generated from the etree.
    The sdc client calls corresponding methods of the manipulator object after every step.
    If the method returns something different from None, this returned value will be used as input for the next step.
    """

    def manipulate_soapenvelope(self, soap_envelope: Soap12Envelope) -> Union[Soap12Envelope, None]:
        ...

    def manipulate_domtree(self, domtree: Element) -> Union[Element, None]:
        ...

    def manipulate_string(self, xml_string: str) -> Union[str,None]:
        ...
