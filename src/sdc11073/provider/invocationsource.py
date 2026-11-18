"""Determination of ``msg:InvocationSource`` for operation invoked reports.

IEEE Std 11073-20701-2018, 7.2.2 (SDC glue) defines how an SDC SERVICE PROVIDER identifies the SDC
PARTICIPANT that invoked a SERVICE OPERATION when it fills
``msg:OperationInvokedReportPart/msg:InvocationSource``:

- **R0077**: an unknown (not authenticated) participant is identified by a fixed anonymous instance
  identifier.
- **R0078**: a known participant is identified by the Common Name of the Distinguished Name of its
  x.509 certificate, using a dedicated ``@Root``.

Identifying a known participant by its Common Name is under discussion, so both identifications are offered as
``SdcProviderComponents.invocation_source_factory``:

- :func:`mk_pem_certificate_invocation_source` (default) identifies it by its entire PEM encoded certificate, as
  proposed by the discussed corrigendum of R0078.
- :func:`mk_common_name_invocation_source` identifies it by the Common Name of its certificate, as the released R0078
  requires. It needs the optional dependency ``cryptography`` (``pip install sdc11073[cryptography]``).
"""

from __future__ import annotations

import ssl

from sdc11073.namespaces import PrefixesEnum
from sdc11073.xml_types import pm_types

try:
    from cryptography import x509
except ImportError:
    _CRYPTOGRAPHY_AVAILABLE = False
else:
    _CRYPTOGRAPHY_AVAILABLE = True

# IEEE Std 11073-20701-2018, 7.2.2
_SDC_NAMESPACE = PrefixesEnum.SDC.namespace

#: R0077 - ``@Root`` of the instance identifier that identifies an unknown SDC PARTICIPANT.
ANONYMOUS_PARTICIPANT_ROOT = _SDC_NAMESPACE
#: R0077 - ``@Extension`` of the instance identifier that identifies an unknown SDC PARTICIPANT.
ANONYMOUS_PARTICIPANT_EXTENSION = 'AnonymousSdcParticipant'
#: R0078 - ``@Root`` of the instance identifier that identifies a known SDC PARTICIPANT by its Common Name.
DISTINGUISHED_NAME_ROOT = f'{_SDC_NAMESPACE}/DistinguishedName'
#: discussed corrigendum of R0078 - ``@Root`` of the instance identifier that identifies a known SDC PARTICIPANT by
#: its PEM encoded x.509 certificate.
X509_CERTIFICATE_PEM_ROOT = f'{_SDC_NAMESPACE}/X509Certificate/PEM'


def _require_cryptography() -> None:
    """Raise an ImportError if the optional dependency cryptography is not installed."""
    if not _CRYPTOGRAPHY_AVAILABLE:
        raise ImportError(
            'identifying an SDC PARTICIPANT by the Common Name of its certificate requires the optional dependency '
            'cryptography, install it with "pip install sdc11073[cryptography]"',
            name='cryptography',
        )


def get_common_names(der_certificate: bytes) -> list[str]:
    """Return the Common Names of the subject Distinguished Name of a DER encoded x.509 certificate.

    Requires the optional dependency cryptography (``pip install sdc11073[cryptography]``).

    :param der_certificate: the certificate, e.g. as returned by ``ssl.SSLSocket.getpeercert(binary_form=True)``.
    :return: the Common Names in the order of the Distinguished Name, i.e. the most specific one is the last one.
    :raises ImportError: if the optional dependency cryptography is not installed.
    :raises ValueError: if der_certificate is not a DER encoded x.509 certificate.
    """
    _require_cryptography()
    subject = x509.load_der_x509_certificate(der_certificate).subject
    return [attribute.value for attribute in subject.get_attributes_for_oid(x509.NameOID.COMMON_NAME)]


def mk_anonymous_invocation_source() -> pm_types.InstanceIdentifier:
    """Return the instance identifier that identifies an unknown SDC PARTICIPANT (R0077)."""
    return pm_types.InstanceIdentifier(
        ANONYMOUS_PARTICIPANT_ROOT,
        extension_string=ANONYMOUS_PARTICIPANT_EXTENSION,
    )


def mk_common_name_invocation_source(peer_certificate: bytes | None) -> pm_types.InstanceIdentifier:
    """Identify the SDC PARTICIPANT that invoked a service operation by the Common Name of its certificate (R0078).

    If the subject of the certificate has more than one Common Name, the most specific (last) one is used, as
    RFC 2818, 3.1 does for server identities. A participant without certificate or with a certificate without
    Common Name is identified by the anonymous instance identifier (R0077).

    Requires the optional dependency cryptography (``pip install sdc11073[cryptography]``).

    :param peer_certificate: the DER encoded x.509 certificate of the participant, None if it is not authenticated.
    :raises ImportError: if the optional dependency cryptography is not installed.
    """
    # checked also without certificate, so that a missing dependency is noticed regardless of the connection
    _require_cryptography()
    common_names = get_common_names(peer_certificate) if peer_certificate else []
    if common_names:
        return pm_types.InstanceIdentifier(DISTINGUISHED_NAME_ROOT, extension_string=common_names[-1])
    return mk_anonymous_invocation_source()


def mk_pem_certificate_invocation_source(peer_certificate: bytes | None) -> pm_types.InstanceIdentifier:
    """Identify the SDC PARTICIPANT that invoked a service operation by its entire PEM encoded certificate.

    This implements the discussed corrigendum of R0078 (https://sourceforge.net/p/opensdc/ieee11073-20701/7/) and is
    the default ``SdcProviderComponents.invocation_source_factory``. A participant without certificate is identified
    by the anonymous instance identifier (R0077).

    :param peer_certificate: the DER encoded x.509 certificate of the participant, None if it is not authenticated.
    """
    if peer_certificate:
        return pm_types.InstanceIdentifier(
            X509_CERTIFICATE_PEM_ROOT,
            extension_string=ssl.DER_cert_to_PEM_cert(peer_certificate),
        )
    return mk_anonymous_invocation_source()
