"""Tests for :mod:`sdc11073.provider.porttypes.invocationsource` (IEEE 11073-20701 R0077/R0078)."""

from __future__ import annotations

import pathlib
import ssl

import pytest

from sdc11073.provider.porttypes import invocationsource

# self-signed v1 certificate (without version field) with the subject Common Name 'CName', used by the tls tests
_TEST_CERTIFICATE_PEM = pathlib.Path(__file__).parent.joinpath('certificates', 'test_certificate.pem').read_text()
_TEST_CERTIFICATE = ssl.PEM_cert_to_DER_cert(_TEST_CERTIFICATE_PEM)
_COMMON_NAME = bytes((0x55, 0x04, 0x03))  # OID 2.5.4.3
_ORGANIZATION_NAME = bytes((0x55, 0x04, 0x0A))  # OID 2.5.4.10
_UTF8_STRING, _PRINTABLE_STRING, _BMP_STRING = 0x0C, 0x13, 0x1E


def _der(tag: int, *contents: bytes) -> bytes:
    """Encode a DER element."""
    content = b''.join(contents)
    if len(content) < 0x80:
        return bytes((tag, len(content))) + content
    length = len(content).to_bytes((len(content).bit_length() + 7) // 8, 'big')
    return bytes((tag, 0x80 | len(length))) + length + content


def _name(*relative_distinguished_names: list[tuple[bytes, int, bytes]]) -> bytes:
    """Encode a Distinguished Name, each relative distinguished name is a list of (oid, string tag, value)."""
    return _der(
        0x30,
        *(
            _der(0x31, *(_der(0x30, _der(0x06, oid), _der(tag, value)) for oid, tag, value in rdn))
            for rdn in relative_distinguished_names
        ),
    )


def _mk_certificate(*subject: list[tuple[bytes, int, bytes]], with_version: bool = True) -> bytes:
    """Build a DER encoded (not validly signed) x.509 certificate with the given subject."""
    algorithm = _der(0x30, _der(0x06, bytes.fromhex('2a864886f70d01010b')), _der(0x05))  # sha256WithRSAEncryption
    tbs_certificate = _der(
        0x30,
        _der(0xA0, _der(0x02, b'\x02')) if with_version else b'',  # version v3
        _der(0x02, b'\x01'),  # serialNumber
        algorithm,  # signature
        _name([(_COMMON_NAME, _UTF8_STRING, b'Test CA')]),  # issuer
        _der(0x30, _der(0x17, b'260101000000Z'), _der(0x17, b'360101000000Z')),  # validity
        _name(*subject),
        _der(0x30, algorithm, _der(0x03, b'\x00')),  # subjectPublicKeyInfo
    )
    return _der(0x30, tbs_certificate, algorithm, _der(0x03, b'\x00'))


class TestGetCommonNames:
    def test_test_certificate(self):
        assert invocationsource.get_common_names(_TEST_CERTIFICATE) == ['CName']

    @pytest.mark.parametrize('with_version', [True, False])
    def test_common_names_in_order_of_distinguished_name(self, with_version: bool):
        certificate = _mk_certificate(
            [(_ORGANIZATION_NAME, _PRINTABLE_STRING, b'Test Company')],
            [(_COMMON_NAME, _UTF8_STRING, b'first')],
            [(_ORGANIZATION_NAME, _UTF8_STRING, b'Unit'), (_COMMON_NAME, _PRINTABLE_STRING, b'second')],
            [(_COMMON_NAME, _BMP_STRING, 'thïrd'.encode('utf-16-be'))],
            with_version=with_version,
        )
        assert invocationsource.get_common_names(certificate) == ['first', 'second', 'thïrd']

    def test_long_common_name(self):
        common_name = 'ü' * 200  # needs the long form of the DER length
        certificate = _mk_certificate([(_COMMON_NAME, _UTF8_STRING, common_name.encode())])
        assert invocationsource.get_common_names(certificate) == [common_name]

    def test_no_common_name(self):
        certificate = _mk_certificate([(_ORGANIZATION_NAME, _UTF8_STRING, b'Test Company')])
        assert invocationsource.get_common_names(certificate) == []

    @pytest.mark.parametrize(
        'data',
        [b'', b'\x30', b'\x30\x05\x30\x00', b'\x30\x02\x30\x00', _TEST_CERTIFICATE[:-1]],
        ids=['empty', 'missing_length', 'truncated_content', 'empty_certificate', 'truncated_certificate'],
    )
    def test_invalid_certificate(self, data: bytes):
        with pytest.raises(ValueError, match='DER'):
            invocationsource.get_common_names(data)


class TestMkCommonNameInvocationSource:
    def test_known_participant(self):
        instance_identifier = invocationsource.mk_common_name_invocation_source(_TEST_CERTIFICATE)
        # IEEE Std 11073-20701-2018, 7.2.2, R0078
        assert instance_identifier.Root == (
            'http://standards.ieee.org/downloads/11073/11073-20701-2018/DistinguishedName'
        )
        assert instance_identifier.Extension == 'CName'

    def test_most_specific_common_name(self):
        certificate = _mk_certificate([(_COMMON_NAME, _UTF8_STRING, b'first')], [(_COMMON_NAME, _UTF8_STRING, b'last')])
        assert invocationsource.mk_common_name_invocation_source(certificate).Extension == 'last'

    @pytest.mark.parametrize(
        'certificate',
        [None, _mk_certificate([(_ORGANIZATION_NAME, _UTF8_STRING, b'Test Company')])],
        ids=['no_certificate', 'no_common_name'],
    )
    def test_anonymous_participant(self, certificate: bytes | None):
        instance_identifier = invocationsource.mk_common_name_invocation_source(certificate)
        # IEEE Std 11073-20701-2018, 7.2.2, R0077
        assert instance_identifier.Root == 'http://standards.ieee.org/downloads/11073/11073-20701-2018'
        assert instance_identifier.Extension == 'AnonymousSdcParticipant'


class TestMkPemCertificateInvocationSource:
    def test_known_participant(self):
        instance_identifier = invocationsource.mk_pem_certificate_invocation_source(_TEST_CERTIFICATE)
        assert instance_identifier.Root == (
            'http://standards.ieee.org/downloads/11073/11073-20701-2018/X509Certificate/PEM'
        )
        assert instance_identifier.Extension == _TEST_CERTIFICATE_PEM

    def test_anonymous_participant(self):
        instance_identifier = invocationsource.mk_pem_certificate_invocation_source(None)
        assert instance_identifier.Root == invocationsource.ANONYMOUS_PARTICIPANT_ROOT
        assert instance_identifier.Extension == invocationsource.ANONYMOUS_PARTICIPANT_EXTENSION
