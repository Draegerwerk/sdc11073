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

import ssl

from sdc11073.namespaces import PrefixesEnum
from sdc11073.xml_types import pm_types

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

# The Common Name is read from the DER encoded certificate by the minimal DER reader below. Using the `cryptography`
# package would be cleaner (x509.load_der_x509_certificate(...).subject.get_attributes_for_oid(...)), but it was
# removed as a dependency of sdc11073 in version 2 and is deliberately not re-added just for this.
_TAG_VERSION = 0xA0  # context specific tag [0] of the version of a TBSCertificate, absent in v1 certificates
_OID_COMMON_NAME = bytes((0x55, 0x04, 0x03))  # 2.5.4.3, id-at-commonName
# UTF8String, UniversalString and BMPString; all other string types have single byte characters (as in OpenSSL)
_STRING_ENCODINGS = {0x0C: 'utf-8', 0x1C: 'utf-32-be', 0x1E: 'utf-16-be'}


def _der_elements(data: bytes) -> list[tuple[int, bytes]]:
    """Split data into its consecutive DER elements and return the tag and the content of each of them."""
    elements = []
    offset = 0
    while offset < len(data):
        if offset + 2 > len(data):
            raise ValueError('truncated DER element')
        tag, length, offset = data[offset], data[offset + 1], offset + 2
        if length & 0x80:  # long form: the low 7 bits are the number of length octets that follow
            length_octets = length & 0x7F
            length = int.from_bytes(data[offset : offset + length_octets], 'big')
            offset += length_octets
        if offset + length > len(data):
            raise ValueError('truncated DER element')
        elements.append((tag, data[offset : offset + length]))
        offset += length
    return elements


def get_common_names(der_certificate: bytes) -> list[str]:
    """Return the Common Names of the subject Distinguished Name of a DER encoded x.509 certificate.

    :param der_certificate: the certificate, e.g. as returned by ``ssl.SSLSocket.getpeercert(binary_form=True)``.
    :return: the Common Names in the order of the Distinguished Name, i.e. the most specific one is the last one.
    :raises ValueError: if der_certificate is not a DER encoded x.509 certificate.
    """
    try:
        # Certificate ::= SEQUENCE { tbsCertificate, signatureAlgorithm, signatureValue }
        certificate = _der_elements(der_certificate)[0][1]
        # TBSCertificate ::= SEQUENCE { [0] version OPTIONAL, serialNumber, signature, issuer, validity, subject, ...}
        tbs_certificate = _der_elements(_der_elements(certificate)[0][1])
        subject = tbs_certificate[5 if tbs_certificate[0][0] == _TAG_VERSION else 4][1]
    except IndexError as ex:
        raise ValueError('not a DER encoded x.509 certificate') from ex
    common_names = []
    # Name ::= SEQUENCE OF RelativeDistinguishedName, RelativeDistinguishedName ::= SET OF AttributeTypeAndValue
    for _, relative_distinguished_name in _der_elements(subject):
        for _, attribute_type_and_value in _der_elements(relative_distinguished_name):
            (_, attribute_type), (value_tag, value) = _der_elements(attribute_type_and_value)
            if attribute_type == _OID_COMMON_NAME:
                common_names.append(value.decode(_STRING_ENCODINGS.get(value_tag, 'latin-1')))
    return common_names


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

    :param peer_certificate: the DER encoded x.509 certificate of the participant, None if it is not authenticated.
    """
    common_names = get_common_names(peer_certificate) if peer_certificate else []
    if common_names:
        return pm_types.InstanceIdentifier(DISTINGUISHED_NAME_ROOT, extension_string=common_names[-1])
    return mk_anonymous_invocation_source()


def mk_pem_certificate_invocation_source(peer_certificate: bytes | None) -> pm_types.InstanceIdentifier:
    """Identify the SDC PARTICIPANT that invoked a service operation by its entire PEM encoded certificate.

    This implements the discussed corrigendum of R0078 (https://sourceforge.net/p/opensdc/ieee11073-20701/7/).
    A participant without certificate is identified by the anonymous instance identifier (R0077).

    :param peer_certificate: the DER encoded x.509 certificate of the participant, None if it is not authenticated.
    """
    if peer_certificate:
        return pm_types.InstanceIdentifier(
            X509_CERTIFICATE_PEM_ROOT,
            extension_string=ssl.DER_cert_to_PEM_cert(peer_certificate),
        )
    return mk_anonymous_invocation_source()
