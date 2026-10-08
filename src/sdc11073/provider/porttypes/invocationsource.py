"""Determination of ``msg:InvocationSource`` for operation invoked reports.

IEEE Std 11073-20701-2018, 7.2.2 (SDC glue) defines how an SDC SERVICE PROVIDER identifies the SDC
PARTICIPANT that invoked a SERVICE OPERATION when it fills
``msg:OperationInvokedReportPart/msg:InvocationSource``:

- **R0077**: an unknown (not authenticated) participant is identified by a fixed anonymous instance
  identifier.
- **R0078**: a known participant is identified by the Common Name of the Distinguished Name of its
  x.509 certificate, using a dedicated ``@Root``.
"""

from __future__ import annotations

from sdc11073.namespaces import PrefixesEnum
from sdc11073.xml_types import pm_types

# IEEE Std 11073-20701-2018, 7.2.2
_SDC_NAMESPACE = PrefixesEnum.SDC.namespace

#: R0077 - ``@Root`` of the instance identifier that identifies an unknown SDC PARTICIPANT.
ANONYMOUS_PARTICIPANT_ROOT = _SDC_NAMESPACE
#: R0077 - ``@Extension`` of the instance identifier that identifies an unknown SDC PARTICIPANT.
ANONYMOUS_PARTICIPANT_EXTENSION = 'AnonymousSdcParticipant'
#: R0078 - ``@Root`` of the instance identifier that identifies a known SDC PARTICIPANT.
DISTINGUISHED_NAME_ROOT = f'{_SDC_NAMESPACE}/DistinguishedName'


def get_common_name(peer_certificate: dict | None) -> str | None:
    """Return the Common Name of the subject Distinguished Name of a decoded x.509 certificate.

    :param peer_certificate: the certificate as returned by :meth:`ssl.SSLSocket.getpeercert`, or None.
    :return: the Common Name, or None if no certificate was provided or it contains no Common Name.
    """
    if not peer_certificate:
        return None
    for relative_distinguished_name in peer_certificate.get('subject', ()):
        for attribute_type, attribute_value in relative_distinguished_name:
            if attribute_type == 'commonName':
                return attribute_value
    return None


def mk_anonymous_invocation_source() -> pm_types.InstanceIdentifier:
    """Return the instance identifier that identifies an unknown SDC PARTICIPANT (R0077)."""
    return pm_types.InstanceIdentifier(
        ANONYMOUS_PARTICIPANT_ROOT,
        extension_string=ANONYMOUS_PARTICIPANT_EXTENSION,
    )


def mk_invocation_source(peer_certificate: dict | None) -> pm_types.InstanceIdentifier:
    """Determine ``msg:InvocationSource`` for the participant that invoked a service operation.

    A participant that presented a verified x.509 client certificate is a known participant and is
    identified by the Common Name of that certificate (R0078). Any other participant is identified by
    the fixed anonymous instance identifier (R0077).

    :param peer_certificate: the client certificate as returned by :meth:`ssl.SSLSocket.getpeercert`,
                             or None if the connection is not mutually authenticated.
    """
    common_name = get_common_name(peer_certificate)
    if common_name:
        return pm_types.InstanceIdentifier(DISTINGUISHED_NAME_ROOT, extension_string=common_name)
    return mk_anonymous_invocation_source()
