from __future__ import annotations
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from lxml.etree import ElementBase
    from sdc11073.pysoap.soapenvelope import Soap12Envelope


class RequestManipulator:
    """
    Example implementation of RequestManipulatorProtocol.
    """
    def __init__(self, cb_soapenvelope=None, cb_xml=None, cb_string=None):
        """

        :param cb_soapenvelope: a callback that gets the SoapEnvelope instance as parameter
        :param cb_xml:  a callback that gets the etree instance as parameter
        :param cb_string:  a callback that gets the output string as parameter
        """
        self.cb_soapenvelope = cb_soapenvelope
        self.cb_xml = cb_xml
        self.cb_string = cb_string

    def manipulate_soapenvelope(self, soap_envelope: Soap12Envelope) -> Union[Soap12Envelope, None]:
        if callable(self.cb_soapenvelope):
            return self.cb_soapenvelope(soap_envelope)
        return None

    def manipulate_domtree(self, domtree: ElementBase) -> Union[ElementBase, None]:
        if callable(self.cb_xml):
            return self.cb_xml(domtree)
        return None

    def manipulate_string(self, xml_string: str) -> Union[str, None]:
        if callable(self.cb_string):
            return self.cb_string(xml_string)
        return None
